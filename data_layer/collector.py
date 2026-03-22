"""
Collector — Data Layer
Runs every 15 minutes during market hours.
Pulls full options chain snapshots from Schwab and writes them to TimescaleDB.

Five components:
  1. RateLimiter         — prevents Schwab API throttling
  2. pull_chain_with_retry() — fetches chain with exponential backoff
  3. validate_chain()    — checks response quality before writing
  4. write_chain_to_db() — maps Schwab JSON → database rows
  5. run_collection_cycle() — main loop across watchlist
"""

import ast
import json
import time
import random
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text

from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    HARD_RULES,
)

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Watchlist ────────────────────────────────────────────────────────────────
# Start small. Add symbols only after confirming clean collection for 5 days.
# All symbols here must have liquid options (high ADV, tight spreads).
WATCHLIST = [
    "IWM",   # Russell 2000        — good IV rank variance
    "NVDA",  # High IV, liquid     — good premium candidate
    "AAPL",  # Defensive, liquid
    "VOO",   # S&P 500 ETF         — equity positions
    "BAC",   # Bank of America     — equity positions
    "GOOG",  # Alphabet            — large-cap, liquid options
    "SMCI",  # Super Micro Computer — high IV, covered-call position
    "SMH",   # VanEck Semis ETF    — high IV, liquid
    "SOXX",  # iShares Semis ETF   — high IV, liquid
    "UNH",   # UnitedHealth        — defensive, high premium
]

# Minimum strikes expected in a healthy chain response for liquid underlyings.
# Below this threshold the chain is considered partial or failed.
MIN_STRIKES_OK      = 20
MIN_STRIKES_PARTIAL = 5


# ── Component 1: Rate Limiter ────────────────────────────────────────────────
class RateLimiter:
    """
    Enforces a minimum delay between API calls to stay under Schwab's
    rate limit of 120 order-related requests per minute.

    For data calls (chain fetches) Schwab is more lenient, but we still
    throttle to be a good API citizen and avoid 429 errors.

    Base delay: 1.5 seconds between calls
    Jitter: ±0.3 seconds random — so calls don't look like a clock
    """

    def __init__(self, base_delay: float = 1.5, jitter: float = 0.3):
        self.base_delay = base_delay
        self.jitter     = jitter
        self._last_call = 0.0

    def wait(self) -> None:
        """Call this before every Schwab API request."""
        elapsed = time.monotonic() - self._last_call
        delay   = self.base_delay + random.uniform(-self.jitter, self.jitter)
        sleep_for = max(0.0, delay - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)
        self._last_call = time.monotonic()


# Module-level rate limiter shared across all calls in this process
_rate_limiter = RateLimiter()


# ── Component 2: Chain Fetcher with Retry ────────────────────────────────────
def pull_chain_with_retry(
    client,
    symbol: str,
    max_retries: int = 3,
) -> dict:
    """
    Fetches a full options chain for `symbol` from Schwab.
    Retries up to max_retries times with exponential backoff on failure.

    Returns the raw chain dict on success, or {} on complete failure.
    Never raises — caller always gets a dict back.

    Exponential backoff schedule:
      Attempt 1 fails → wait 2s
      Attempt 2 fails → wait 4s
      Attempt 3 fails → wait 8s → return {}
    """
    for attempt in range(1, max_retries + 1):
        try:
            _rate_limiter.wait()

            response = client.get_option_chain(
                symbol=symbol,
                contract_type=client.Options.ContractType.ALL,
                include_underlying_quote=True,
            )

            # Raise immediately on HTTP errors (4xx, 5xx)
            response.raise_for_status()

            chain = response.json()

            if not chain:
                logger.warning(f"{symbol}: empty response on attempt {attempt}")
                raise ValueError("Empty chain response")

            logger.debug(f"{symbol}: chain fetched successfully on attempt {attempt}")
            return chain

        except Exception as e:
            wait_time = 2 ** attempt  # 2, 4, 8 seconds
            if attempt < max_retries:
                logger.warning(
                    f"{symbol}: fetch attempt {attempt} failed ({e}) — "
                    f"retrying in {wait_time}s"
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    f"{symbol}: all {max_retries} fetch attempts failed — "
                    f"skipping this symbol for this cycle"
                )

    return {}  # Return empty dict after all retries exhausted


# ── Component 3: Chain Validator ─────────────────────────────────────────────
def validate_chain(chain: dict, symbol: str) -> str:
    """
    Checks whether the chain response is usable before writing to DB.

    Returns one of three quality states:
      "ok"      — full chain, safe to use for strategy scoring
      "partial" — fewer strikes than expected, write but flag it
      "failed"  — unusable, do not write

    Why this matters: Schwab can return HTTP 200 with an empty or malformed
    chain during market open/close transitions, halts, or API issues.
    Writing bad data to the DB would corrupt your strategy signals.
    """
    if not chain:
        return "failed"

    # Check underlying price is present and non-zero
    underlying_price = chain.get("underlyingPrice")
    if not underlying_price or underlying_price <= 0:
        logger.warning(f"{symbol}: no valid underlying price in chain")
        return "failed"

    # Count total strikes across all expirations
    puts  = chain.get("putExpDateMap",  {})
    calls = chain.get("callExpDateMap", {})

    total_put_strikes  = sum(len(strikes) for strikes in puts.values())
    total_call_strikes = sum(len(strikes) for strikes in calls.values())
    total_strikes      = total_put_strikes + total_call_strikes

    if total_strikes >= MIN_STRIKES_OK:
        return "ok"
    elif total_strikes >= MIN_STRIKES_PARTIAL:
        logger.warning(
            f"{symbol}: partial chain — only {total_strikes} strikes returned"
        )
        return "partial"
    else:
        logger.error(
            f"{symbol}: chain failed validation — "
            f"{total_strikes} strikes (minimum {MIN_STRIKES_PARTIAL} required)"
        )
        return "failed"


# ── Required contracts from open positions ───────────────────────────────────
def _load_required_contracts(conn) -> set[tuple[str, str, str, float]]:
    """
    Load (symbol, expiry, option_right, strike) tuples for all option contracts
    referenced by open positions. Used to force-include these in option_quotes
    even when outside the normal DTE window.
    """
    required: set[tuple[str, str, str, float]] = set()
    try:
        rows = conn.execute(text("""
            SELECT symbol, expiry, strategy,
                   long_put_strike, short_put_strike,
                   short_call_strike, long_call_strike,
                   legs_json
            FROM positions
            WHERE status = 'open'
              AND strategy IN (
                'IRON_CONDOR', 'SHORT_OPTION', 'LONG_OPTION',
                'VERTICAL_SPREAD', 'STRADDLE', 'STRANGLE'
              )
        """)).fetchall()
    except Exception as e:
        logger.warning(f"collector: could not load required contracts — {e}")
        return required

    for row in rows:
        sym = str(row[0]) if row[0] else None
        exp = str(row[1])[:10] if row[1] else None
        strategy = (row[2] or "").upper()
        lp, sp, sc, lc = row[3], row[4], row[5], row[6]
        legs_raw = row[7] if len(row) > 7 else None

        if not sym:
            continue

        def _add(side: str, strike_val, expiry_val: str | None) -> None:
            if strike_val is not None and expiry_val:
                try:
                    s = round(float(strike_val), 4)
                    required.add((sym, expiry_val, side, s))
                except (TypeError, ValueError):
                    pass

        # Parse legs_json for single-leg and multi-leg strategies
        legs: list = []
        if legs_raw is not None:
            if isinstance(legs_raw, list):
                legs = legs_raw
            elif isinstance(legs_raw, dict):
                legs = [legs_raw]
            elif isinstance(legs_raw, str) and legs_raw.strip():
                try:
                    parsed = json.loads(legs_raw)
                except (json.JSONDecodeError, ValueError, TypeError):
                    try:
                        parsed = ast.literal_eval(legs_raw)
                    except (ValueError, SyntaxError, TypeError):
                        parsed = None
                if isinstance(parsed, list):
                    legs = parsed
                elif isinstance(parsed, dict):
                    legs = [parsed]

        for leg in legs:
            if isinstance(leg, dict):
                strike = leg.get("strike")
                right = leg.get("option_type") or leg.get("right")
                leg_exp = leg.get("expiry")
                if strike is not None and right:
                    exp_val = str(leg_exp)[:10] if leg_exp else exp
                    if exp_val:
                        _add(str(right).upper()[:1] or "P", strike, exp_val)

        # IRON_CONDOR: use strike columns when legs_json doesn't cover
        if strategy == "IRON_CONDOR" and exp:
            if lp is not None:
                _add("P", lp, exp)
            if sp is not None:
                _add("P", sp, exp)
            if sc is not None:
                _add("C", sc, exp)
            if lc is not None:
                _add("C", lc, exp)

    return required


# ── Component 4: Database Writer ─────────────────────────────────────────────
def write_chain_to_db(
    conn,
    snapshot_id: int,
    symbol: str,
    chain: dict,
    collected_at: datetime,
    required_contracts: set[tuple[str, str, str, float]] | None = None,
) -> int:
    """
    Writes one full options chain snapshot to the database.

    Writes to two tables:
      underlying_quotes — one row per symbol per snapshot (price + raw JSON)
      option_quotes     — one row per contract per snapshot

    Column mapping (Schwab JSON → actual DB schema):
      underlying_quotes: ts, symbol, price, snapshot_id, raw
      option_quotes:     ts, snapshot_id, symbol, expiry, strike,
                         option_right, bid, ask, iv, delta, gamma,
                         theta, vega, volume, open_interest, raw

    Returns the number of contracts written.
    """
    underlying_price = chain.get("underlyingPrice", 0)

    # Write underlying quote row
    conn.execute(text("""
        INSERT INTO underlying_quotes
            (ts, symbol, price, snapshot_id, raw)
        VALUES
            (:ts, :symbol, :price, :snapshot_id, :raw)
        ON CONFLICT DO NOTHING
    """), {
        "ts":          collected_at,
        "symbol":      symbol,
        "price":       underlying_price,
        "snapshot_id": snapshot_id,
        "raw":         None,  # can store full chain JSON here later if needed
    })

    # Write each option contract
    contracts_written = 0

    for side, exp_map in [
        ("P", chain.get("putExpDateMap",  {})),
        ("C", chain.get("callExpDateMap", {})),
    ]:
        for exp_key, strikes in exp_map.items():
            # exp_key format: "2026-04-17:37" (date:dte)
            try:
                expiry_str = exp_key.split(":")[0]   # "2026-04-17"
                dte        = int(exp_key.split(":")[1])
            except (IndexError, ValueError):
                logger.warning(f"{symbol}: could not parse expiry key '{exp_key}'")
                continue

            # Force-include contracts required by open positions; otherwise DTE window
            rc = required_contracts or set()
            in_window = HARD_RULES["min_dte"] <= dte <= HARD_RULES["max_dte"]

            for strike_str, contracts in strikes.items():
                if not contracts:
                    continue

                c = contracts[0]  # Schwab wraps each strike in a list

                bid           = c.get("bid",          None)
                ask           = c.get("ask",          None)
                volume        = c.get("totalVolume",  None)
                open_interest = c.get("openInterest", None)
                delta         = c.get("delta",        None)
                gamma         = c.get("gamma",        None)
                theta         = c.get("theta",        None)
                vega          = c.get("vega",         None)
                iv            = c.get("volatility",   None)

                try:
                    strike = float(strike_str)
                except ValueError:
                    continue

                strike_rounded = round(strike, 4)
                is_required = (symbol, expiry_str, side, strike_rounded) in rc
                if not in_window and not is_required:
                    continue

                conn.execute(text("""
                    INSERT INTO option_quotes (
                        ts, snapshot_id, symbol, expiry, dte, strike,
                        option_right, bid, ask, iv,
                        delta, gamma, theta, vega,
                        volume, open_interest, raw
                    ) VALUES (
                        :ts, :snapshot_id, :symbol, :expiry, :dte, :strike,
                        :option_right, :bid, :ask, :iv,
                        :delta, :gamma, :theta, :vega,
                        :volume, :open_interest, :raw
                    )
                    ON CONFLICT DO NOTHING
                """), {
                    "ts":            collected_at,
                    "snapshot_id":   snapshot_id,
                    "symbol":        symbol,
                    "expiry":        expiry_str,
                    "dte":           dte,
                    "strike":        strike,
                    "option_right":  side,
                    "bid":           bid,
                    "ask":           ask,
                    "iv":            iv,
                    "delta":         delta,
                    "gamma":         gamma,
                    "theta":         theta,
                    "vega":          vega,
                    "volume":        volume,
                    "open_interest": open_interest,
                    "raw":           None,
                })
                contracts_written += 1
            

    return contracts_written


# ── Component 5: Main Collection Loop ────────────────────────────────────────
def run_collection_cycle(client) -> dict:
    """
    Main entry point — called every 15 minutes by the scheduler in main.py.

    For each symbol in WATCHLIST:
      1. Fetch chain with retry
      2. Validate chain quality
      3. Write to database if ok or partial
      4. Log result

    snapshot_runs uses a simple schema — provider, status, meta (JSONB).
    All summary data is stored in the meta field as JSON.

    Returns a summary dict for logging and freshness monitoring.
    """
    collected_at = datetime.now(timezone.utc)
    engine       = create_engine(DB_URL)

    symbols_ok      = []
    symbols_partial = []
    symbols_failed  = []
    total_contracts = 0

    with engine.connect() as conn:

        # Derive dynamic symbol universe:
        #   base WATCHLIST
        #   ∪ all symbols with open non-equity option positions in positions table.
        extra_symbols: list[str] = []
        try:
            rows = conn.execute(text("""
                SELECT DISTINCT symbol
                FROM positions
                WHERE status = 'open'
                  AND strategy IN (
                    'IRON_CONDOR',
                    'SHORT_OPTION',
                    'LONG_OPTION',
                    'VERTICAL_SPREAD',
                    'STRADDLE',
                    'STRANGLE'
                  )
            """)).fetchall()
            extra_symbols = [str(r[0]) for r in rows if r[0]]
        except Exception as e:
            logger.warning(f"collector: could not load open-position symbols — {e}")

        # Ensure we keep the explicit watchlist ordering; open-position symbols
        # are appended and de-duplicated while preserving WATCHLIST order.
        symbols: list[str] = []
        seen: set[str] = set()
        for sym in WATCHLIST + extra_symbols:
            if sym not in seen:
                seen.add(sym)
                symbols.append(sym)

        # Create the snapshot_runs anchor row
        result = conn.execute(text("""
            INSERT INTO snapshot_runs (provider, status, meta)
            VALUES ('schwab', 'running', :meta)
            RETURNING id
        """), {
            "meta": f'{{"started_at": "{collected_at.isoformat()}", '
                    f'"symbols_attempted": {len(symbols)}}}',
        })
        snapshot_id = result.scalar()
        conn.commit()

        required_contracts = _load_required_contracts(conn)
        if required_contracts:
            logger.info(
                f"Snapshot {snapshot_id}: force-including {len(required_contracts)} "
                "required contracts from open positions"
            )

        logger.info(
            f"Snapshot {snapshot_id} started — "
            f"collecting {len(symbols)} symbols "
            f"(base_watchlist={len(WATCHLIST)}, from_open_positions={len(extra_symbols)})"
        )

        # Loop through effective symbol universe (watchlist ∪ open-position symbols)
        for symbol in symbols:
            try:
                # Step 1: Fetch
                chain = pull_chain_with_retry(client, symbol)

                # Step 2: Validate
                quality = validate_chain(chain, symbol)

                if quality == "failed":
                    symbols_failed.append(symbol)
                    logger.warning(f"{symbol}: skipped — chain quality: failed")
                    continue

                # Step 3: Write
                n = write_chain_to_db(
                    conn, snapshot_id, symbol, chain, collected_at,
                    required_contracts=required_contracts,
                )
                conn.commit()
                total_contracts += n

                if quality == "ok":
                    symbols_ok.append(symbol)
                    logger.info(f"{symbol}: ✓ {n} contracts written")
                else:
                    symbols_partial.append(symbol)
                    logger.warning(f"{symbol}: ⚠ {n} contracts written (partial)")

            except Exception as e:
                symbols_failed.append(symbol)
                logger.error(f"{symbol}: unexpected error — {e}")
                continue

        # Determine final snapshot status
        final_status = "ok" if not symbols_failed else (
            "partial" if (symbols_ok or symbols_partial) else "failed"
        )

        # Update snapshot_runs with results stored in meta JSONB
        import json
        conn.execute(text("""
            UPDATE snapshot_runs
            SET
                status = :status,
                meta   = :meta
            WHERE id = :id
        """), {
            "status": final_status,
            "meta": json.dumps({
                "started_at":      collected_at.isoformat(),
                "completed_at":    datetime.now(timezone.utc).isoformat(),
                "symbols_ok":      symbols_ok,
                "symbols_partial": symbols_partial,
                "symbols_failed":  symbols_failed,
                "total_contracts": total_contracts,
            }),
            "id": snapshot_id,
        })
        conn.commit()

    summary = {
        "snapshot_id":     snapshot_id,
        "collected_at":    collected_at,
        "symbols_ok":      symbols_ok,
        "symbols_partial": symbols_partial,
        "symbols_failed":  symbols_failed,
        "total_contracts": total_contracts,
    }

    logger.info(
        f"Snapshot {snapshot_id} complete — "
        f"ok:{len(symbols_ok)} partial:{len(symbols_partial)} "
        f"failed:{len(symbols_failed)} contracts:{total_contracts}"
    )

    return summary


# ── Manual test run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    from data_layer.provider import get_schwab_client

    print("Running single collection cycle as manual test...")
    print("Run during market hours (9:30 AM–4:00 PM ET) for real data.\n")

    client  = get_schwab_client()
    summary = run_collection_cycle(client)

    print("\n── Collection Summary ──────────────────────")
    print(f"Snapshot ID:     {summary['snapshot_id']}")
    print(f"Collected at:    {summary['collected_at']}")
    print(f"OK symbols:      {summary['symbols_ok']}")
    print(f"Partial symbols: {summary['symbols_partial']}")
    print(f"Failed symbols:  {summary['symbols_failed']}")
    print(f"Total contracts: {summary['total_contracts']}")
    print("────────────────────────────────────────────")
    print("\nCheck option_quotes and underlying_quotes in TablePlus to verify.")