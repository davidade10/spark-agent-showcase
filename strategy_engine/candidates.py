"""
candidates.py — Strategy Engine
Scans the most recent option_quotes snapshot and identifies
iron condor candidates across the watchlist.

An iron condor has four legs:
  Long  Put  (further OTM)  — protection wing
  Short Put  (closer OTM)   — premium collection
  Short Call (closer OTM)   — premium collection
  Long  Call (further OTM)  — protection wing

Target structure:
  Short strikes: delta ~0.16 (roughly 1 standard deviation OTM)
  Wing width:    $5 spread between short and long on each side
  DTE window:    21–50 days to expiration
  Min credit:    $0.40 net credit to open
  Max width:     $10 spread width

Output: list of IronCondorCandidate dataclasses, sorted by DTE
        (closest to 30-45 DTE sweet spot first).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from sqlalchemy import create_engine, text

from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    HARD_RULES,
)

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Target parameters ─────────────────────────────────────────────────────────
TARGET_SHORT_DELTA  = 0.16   # ideal short strike delta (absolute value)
DELTA_TOLERANCE     = 0.06   # accept strikes with delta 0.10–0.22
WING_WIDTH_DOLLARS  = 5.0    # default spread width ($5 wide on each side)
IDEAL_DTE           = 38     # scoring target (middle of 30–45 sweet spot)


# ── Data structure ────────────────────────────────────────────────────────────
@dataclass
class IronCondorCandidate:
    """
    Represents one fully-defined iron condor setup.
    All four legs are specified. Credit and max loss are pre-computed.
    This object flows into scoring.py → rules_gate.py → llm_layer.
    """

    # Identity
    symbol:       str
    snapshot_id:  int
    expiry:       str           # "2026-04-17"
    dte:          int           # days to expiration at time of scan

    # Four legs — strikes
    long_put_strike:   float    # lowest strike  (protection)
    short_put_strike:  float    # second lowest  (premium)
    short_call_strike: float    # second highest (premium)
    long_call_strike:  float    # highest strike (protection)

    # Four legs — mid prices (used for credit calculation)
    long_put_mid:   float
    short_put_mid:  float
    short_call_mid: float
    long_call_mid:  float

    # Greeks at short strikes
    short_put_delta:  float     # should be negative, abs ~0.16
    short_call_delta: float     # should be positive, abs ~0.16

    # Economics
    net_credit:   float         # total premium collected (per share)
    spread_width: float         # width of each spread in dollars
    max_loss:     float         # spread_width - net_credit (per share)

    # Context
    underlying_price: float
    iv_rank:          Optional[float] = None  # None until 3 months of history

    # Position size — condors (contracts per leg); scanner defaults to 1-lot proposals
    qty:              int       = 1

    # Metadata
    scan_notes: list[str] = field(default_factory=list)


@dataclass
class StrangleCandidate:
    """
    One short strangle — sell OTM put and OTM call with no protective wings.
    Credit = sum of both short legs' mid-prices.
    Max loss is theoretically unlimited; excluded from position-risk gate checks.
    Flows into score_strangle() → rules_gate strangle branch → approval UI.
    """

    # Strategy tag (always "STRANGLE")
    strategy:           str

    # Identity
    symbol:             str
    snapshot_id:        int
    expiry:             str            # "2026-04-17"
    dte:                int

    # Two short legs — strikes only, no wings
    short_put_strike:   float
    short_call_strike:  float

    # Greeks at short strikes
    short_put_delta:    float          # negative (put), abs ~0.16
    short_call_delta:   float          # positive (call), abs ~0.16

    # Leg credits (mid-prices)
    short_put_credit:   float
    short_call_credit:  float

    # Economics
    net_credit:         float          # short_put_credit + short_call_credit

    # Context
    iv_rank:            Optional[float]
    underlying_price:   float

    # Position size
    qty:                int            = 1

    # Metadata / context notes
    context_block:      list[str]      = field(default_factory=list)


# ── Expiry helpers ────────────────────────────────────────────────────────────
def _is_monthly_expiry(expiry: str) -> bool:
    """
    Returns True if the expiry is the 3rd Friday of its month
    (standard monthly options expiration — highest open interest / liquidity).
    Weekly expirations fall on other Fridays.
    """
    d = date.fromisoformat(expiry)
    return d.weekday() == 4 and 15 <= d.day <= 21


# ── Database queries ──────────────────────────────────────────────────────────
def _get_latest_snapshot_id(conn) -> Optional[int]:
    """
    Returns the ID of the most recent successful snapshot.
    Returns None if no snapshots exist yet.
    """
    row = conn.execute(text("""
        SELECT id FROM snapshot_runs
        WHERE status IN ('ok', 'partial')
        ORDER BY ts DESC
        LIMIT 1
    """)).fetchone()

    return row.id if row else None


def _get_underlying_price(conn, symbol: str, snapshot_id: int) -> Optional[float]:
    """Fetches the underlying spot price for a symbol from the snapshot."""
    row = conn.execute(text("""
        SELECT price FROM underlying_quotes
        WHERE symbol = :symbol
          AND snapshot_id = :snapshot_id
        LIMIT 1
    """), {"symbol": symbol, "snapshot_id": snapshot_id}).fetchone()

    return float(row.price) if row else None


def _get_open_positions_by_symbol(conn) -> dict[str, list[str]]:
    """
    Returns symbol -> list of open strategy names.
    Uses the same definition of 'open' as the Positions tab (status='open').
    """
    rows = conn.execute(text("""
        SELECT symbol, UPPER(COALESCE(strategy, '')) AS strategy
        FROM positions
        WHERE status = 'open'
          AND symbol IS NOT NULL
          AND TRIM(symbol) != ''
    """)).fetchall()

    result: dict[str, list[str]] = {}
    for r in rows:
        sym = r.symbol.strip()
        strat = (r.strategy or "").strip()
        if not strat:
            strat = "UNKNOWN"
        if sym not in result:
            result[sym] = []
        if strat not in result[sym]:
            result[sym].append(strat)
    return result


def _get_iv_rank(conn, symbol: str) -> Optional[float]:
    """
    Fetches the most recently computed IV rank for a symbol.
    Returns None if not yet computed (first 3 months of collection).
    The strategy engine treats None as neutral (score of 0, not disqualifying).
    """
    row = conn.execute(text("""
        SELECT iv_rank FROM underlying_quotes
        WHERE symbol = :symbol
          AND iv_rank IS NOT NULL
        ORDER BY ts DESC
        LIMIT 1
    """), {"symbol": symbol}).fetchone()

    return float(row.iv_rank) if row else None


def _get_options_for_symbol(conn, symbol: str, snapshot_id: int) -> list[dict]:
    """
    Pulls all option contracts for a symbol from the given snapshot,
    filtered to the target DTE window defined in HARD_RULES.

    Returns a list of dicts with all columns needed for candidate construction.
    """
    rows = conn.execute(text("""
        SELECT
            expiry,
            dte,
            strike,
            option_right,
            bid,
            ask,
            delta,
            gamma,
            theta,
            vega,
            iv,
            volume,
            open_interest
        FROM option_quotes
        WHERE symbol      = :symbol
          AND snapshot_id = :snapshot_id
          AND dte BETWEEN :min_dte AND :max_dte
          AND bid IS NOT NULL
          AND ask IS NOT NULL
          AND delta IS NOT NULL
          AND bid >= 0
          AND ask >= bid
        ORDER BY expiry, strike
    """), {
        "symbol":      symbol,
        "snapshot_id": snapshot_id,
        "min_dte":     HARD_RULES["min_dte"],
        "max_dte":     HARD_RULES["max_dte"],
    }).fetchall()

    return [dict(row._mapping) for row in rows]


# ── Leg selection logic ───────────────────────────────────────────────────────
def _mid(row: dict) -> float:
    """Mid-price of a contract. Used for credit calculation."""
    return round((row["bid"] + row["ask"]) / 2, 4)


def _find_short_put(puts: list[dict]) -> Optional[dict]:
    """
    Finds the put closest to TARGET_SHORT_DELTA (0.16) by absolute delta.
    Put deltas from Schwab are negative — we compare absolute values.
    Rejects strikes outside the DELTA_TOLERANCE band (0.10–0.22).
    """
    candidates = [
        p for p in puts
        if p["delta"] is not None
        and abs(p["delta"]) >= (TARGET_SHORT_DELTA - DELTA_TOLERANCE / 2)
        and abs(p["delta"]) <= HARD_RULES["max_short_delta"]
    ]

    if not candidates:
        return None

    return min(candidates, key=lambda p: abs(abs(p["delta"]) - TARGET_SHORT_DELTA))


def _find_short_call(calls: list[dict]) -> Optional[dict]:
    """
    Finds the call closest to TARGET_SHORT_DELTA (0.16).
    Call deltas from Schwab are positive.
    """
    candidates = [
        c for c in calls
        if c["delta"] is not None
        and c["delta"] >= (TARGET_SHORT_DELTA - DELTA_TOLERANCE / 2)
        and c["delta"] <= HARD_RULES["max_short_delta"]
    ]

    if not candidates:
        return None

    return min(candidates, key=lambda c: abs(c["delta"] - TARGET_SHORT_DELTA))


def _find_long_put(puts: list[dict], short_put_strike: float) -> Optional[dict]:
    """
    Finds the long put wing — the put BELOW the short put strike.
    Targets exactly WING_WIDTH_DOLLARS below, falls back to closest available.
    Must be at least $1 below the short strike.
    """
    target_strike = short_put_strike - WING_WIDTH_DOLLARS

    candidates = [
        p for p in puts
        if p["strike"] < short_put_strike - 1.0
    ]

    if not candidates:
        return None

    return min(candidates, key=lambda p: abs(p["strike"] - target_strike))


def _find_long_call(calls: list[dict], short_call_strike: float) -> Optional[dict]:
    """
    Finds the long call wing — the call ABOVE the short call strike.
    Targets exactly WING_WIDTH_DOLLARS above, falls back to closest available.
    Must be at least $1 above the short strike.
    """
    target_strike = short_call_strike + WING_WIDTH_DOLLARS

    candidates = [
        c for c in calls
        if c["strike"] > short_call_strike + 1.0
    ]

    if not candidates:
        return None

    return min(candidates, key=lambda c: abs(c["strike"] - target_strike))


# ── Candidate builder ─────────────────────────────────────────────────────────
def _build_candidate_for_expiry(
    symbol:           str,
    snapshot_id:      int,
    expiry:           str,
    dte:              int,
    contracts:        list[dict],
    underlying_price: float,
    iv_rank:          Optional[float],
    existing_strategies: Optional[list[str]] = None,
) -> Optional[IronCondorCandidate]:
    """
    Attempts to build one IronCondorCandidate for a specific expiry.
    Returns None if any required leg cannot be found or economics fail.

    This function is called once per expiry per symbol.
    """
    puts  = [c for c in contracts if c["option_right"] == "P"]
    calls = [c for c in contracts if c["option_right"] == "C"]

    if not puts or not calls:
        logger.debug(f"{symbol} {expiry}: no puts or calls — skipping")
        return None

    # Find short strikes (delta targeting)
    short_put  = _find_short_put(puts)
    short_call = _find_short_call(calls)

    if not short_put or not short_call:
        logger.debug(
            f"{symbol} {expiry}: could not find short strikes near "
            f"delta {TARGET_SHORT_DELTA}"
        )
        return None

    # Find long wings (spread protection)
    long_put  = _find_long_put(puts,  short_put["strike"])
    long_call = _find_long_call(calls, short_call["strike"])

    if not long_put or not long_call:
        logger.debug(f"{symbol} {expiry}: could not find wing strikes — skipping")
        return None

    # Compute economics
    short_put_mid  = _mid(short_put)
    short_call_mid = _mid(short_call)
    long_put_mid   = _mid(long_put)
    long_call_mid  = _mid(long_call)

    net_credit = round(
        (short_put_mid + short_call_mid) - (long_put_mid + long_call_mid),
        4,
    )

    put_spread_width  = round(short_put["strike"]  - long_put["strike"],  2)
    call_spread_width = round(long_call["strike"] - short_call["strike"], 2)
    spread_width      = max(put_spread_width, call_spread_width)
    max_loss          = round(spread_width - net_credit, 4)

    notes = []

    # Hard rule: minimum net credit
    if net_credit < HARD_RULES["min_net_credit"]:
        logger.debug(
            f"{symbol} {expiry}: net credit ${net_credit:.2f} below "
            f"minimum ${HARD_RULES['min_net_credit']:.2f} — skipping"
        )
        return None

    # Hard rule: maximum spread width
    if spread_width > HARD_RULES["max_spread_width"]:
        logger.debug(
            f"{symbol} {expiry}: spread width ${spread_width:.2f} exceeds "
            f"maximum ${HARD_RULES['max_spread_width']:.2f} — skipping"
        )
        return None

    # Note if wings aren't exactly target width
    if abs(put_spread_width - WING_WIDTH_DOLLARS) > 1.0:
        notes.append(
            f"Put spread width ${put_spread_width:.1f} "
            f"(target ${WING_WIDTH_DOLLARS:.1f})"
        )
    if abs(call_spread_width - WING_WIDTH_DOLLARS) > 1.0:
        notes.append(
            f"Call spread width ${call_spread_width:.1f} "
            f"(target ${WING_WIDTH_DOLLARS:.1f})"
        )

    if iv_rank is None:
        notes.append("IV rank not yet available (< 3 months history)")

    if existing_strategies:
        notes.append(
            f"Existing open position(s) in {symbol}: "
            f"{', '.join(sorted(existing_strategies))}"
        )

    return IronCondorCandidate(
        symbol            = symbol,
        snapshot_id       = snapshot_id,
        expiry            = str(expiry),
        dte               = dte,
        long_put_strike   = long_put["strike"],
        short_put_strike  = short_put["strike"],
        short_call_strike = short_call["strike"],
        long_call_strike  = long_call["strike"],
        long_put_mid      = long_put_mid,
        short_put_mid     = short_put_mid,
        short_call_mid    = short_call_mid,
        long_call_mid     = long_call_mid,
        short_put_delta   = round(short_put["delta"],  4),
        short_call_delta  = round(short_call["delta"], 4),
        net_credit        = net_credit,
        spread_width      = spread_width,
        max_loss          = max_loss,
        underlying_price  = underlying_price,
        iv_rank           = iv_rank,
        scan_notes        = notes,
    )


# ── Strangle builder ──────────────────────────────────────────────────────────
def _build_strangle_for_expiry(
    symbol:              str,
    snapshot_id:         int,
    expiry:              str,
    dte:                 int,
    contracts:           list[dict],
    underlying_price:    float,
    iv_rank:             Optional[float],
    min_credit:          float,
    existing_strategies: Optional[list[str]] = None,
) -> Optional[StrangleCandidate]:
    """
    Attempts to build one StrangleCandidate for a specific expiry.
    Uses the same delta-targeting logic as iron condors (_find_short_put,
    _find_short_call) — best delta match within the [0.10, 0.22] band.
    Returns None if any required leg is missing or net credit is below minimum.
    """
    puts  = [c for c in contracts if c["option_right"] == "P"]
    calls = [c for c in contracts if c["option_right"] == "C"]

    if not puts or not calls:
        logger.debug(f"{symbol} {expiry}: strangle — no puts or calls — skipping")
        return None

    short_put  = _find_short_put(puts)
    short_call = _find_short_call(calls)

    if not short_put or not short_call:
        logger.debug(
            f"{symbol} {expiry}: strangle — could not find short strikes "
            f"near delta {TARGET_SHORT_DELTA}"
        )
        return None

    short_put_credit  = _mid(short_put)
    short_call_credit = _mid(short_call)
    net_credit        = round(short_put_credit + short_call_credit, 4)

    if net_credit < min_credit:
        logger.debug(
            f"{symbol} {expiry}: strangle net credit ${net_credit:.2f} "
            f"below minimum ${min_credit:.2f} — skipping"
        )
        return None

    notes = []
    if iv_rank is None:
        notes.append("IV rank not yet available (< 3 months history)")
    if existing_strategies:
        notes.append(
            f"Existing open position(s) in {symbol}: "
            f"{', '.join(sorted(existing_strategies))}"
        )

    return StrangleCandidate(
        strategy          = "STRANGLE",
        symbol            = symbol,
        snapshot_id       = snapshot_id,
        expiry            = str(expiry),
        dte               = dte,
        short_put_strike  = short_put["strike"],
        short_call_strike = short_call["strike"],
        short_put_delta   = round(short_put["delta"],  4),
        short_call_delta  = round(short_call["delta"], 4),
        short_put_credit  = short_put_credit,
        short_call_credit = short_call_credit,
        net_credit        = net_credit,
        iv_rank           = iv_rank,
        underlying_price  = underlying_price,
        context_block     = notes,
    )


def generate_strangle_candidates(
    conn,
    symbols:        list[str],
    snapshot_id:    int,
    open_positions: dict[str, list[str]],
) -> list[StrangleCandidate]:
    """
    Scans option_quotes for short strangle candidates across all symbols.

    Strangle rules applied here (pre-gate):
      - Symbol must not already have an open STRANGLE position
      - IV rank must be >= strangle_min_iv_rank (HARD_RULES key, fallback 50)
        — strangles carry unlimited risk; elevated IV is required
      - Net credit must be >= strangle_min_credit (HARD_RULES key, fallback 1.50)
      - One candidate per expiry per symbol — best delta match

    Reuses the same DTE window and expiry-preference logic as condor generation.
    """
    try:
        min_iv_rank = float(HARD_RULES["strangle_min_iv_rank"])
    except (KeyError, TypeError, ValueError):
        min_iv_rank = 50.0

    try:
        min_credit = float(HARD_RULES["strangle_min_credit"])
    except (KeyError, TypeError, ValueError):
        min_credit = 1.50

    candidates: list[StrangleCandidate] = []

    for symbol in symbols:
        open_strategies = open_positions.get(symbol, [])

        if "STRANGLE" in open_strategies:
            logger.info(
                f"{symbol}: skipping strangle scan — already has open STRANGLE position"
            )
            continue

        iv_rank = _get_iv_rank(conn, symbol)

        # IV rank gate: strangles require elevated IV to justify unlimited risk
        if iv_rank is not None and iv_rank < min_iv_rank:
            logger.debug(
                f"{symbol}: strangle skipped — IV rank {iv_rank:.1f} "
                f"below minimum {min_iv_rank:.1f}"
            )
            continue

        underlying_price = _get_underlying_price(conn, symbol, snapshot_id)
        if underlying_price is None:
            continue

        contracts = _get_options_for_symbol(conn, symbol, snapshot_id)
        if not contracts:
            continue

        # Group by expiry, prefer monthly expirations (same logic as condors)
        expiries: dict[tuple, list[dict]] = {}
        for c in contracts:
            key = (str(c["expiry"]), int(c["dte"]))
            expiries.setdefault(key, []).append(c)

        expiry_keys = sorted(expiries.keys())
        monthly_keys = [
            (exp, dte) for (exp, dte) in expiry_keys if _is_monthly_expiry(exp)
        ]
        if monthly_keys:
            expiry_keys = monthly_keys

        for (expiry, dte) in expiry_keys:
            candidate = _build_strangle_for_expiry(
                symbol              = symbol,
                snapshot_id         = snapshot_id,
                expiry              = expiry,
                dte                 = dte,
                contracts           = expiries[(expiry, dte)],
                underlying_price    = underlying_price,
                iv_rank             = iv_rank,
                min_credit          = min_credit,
                existing_strategies = open_strategies if open_strategies else None,
            )
            if candidate:
                candidates.append(candidate)

    logger.info(f"Strangle scan: {len(candidates)} candidates across {len(symbols)} symbols")
    return candidates


# ── Main scanner ──────────────────────────────────────────────────────────────
def scan_for_candidates(
    symbols:     Optional[list[str]] = None,
    snapshot_id: Optional[int]       = None,
) -> list[IronCondorCandidate | StrangleCandidate]:
    """
    Main entry point — scans the latest snapshot for iron condor candidates.

    Args:
        symbols:     list of symbols to scan (defaults to all in latest snapshot)
        snapshot_id: specific snapshot to scan (defaults to most recent)

    Returns:
        List of IronCondorCandidate objects sorted by distance from IDEAL_DTE.
        Empty list if no candidates found or no data available.
    """
    engine     = create_engine(DB_URL)
    candidates = []

    with engine.connect() as conn:

        # Use most recent snapshot if not specified
        if snapshot_id is None:
            snapshot_id = _get_latest_snapshot_id(conn)

        if snapshot_id is None:
            logger.warning("No snapshots found — run the collector first")
            return []

        logger.info(f"Scanning snapshot_id={snapshot_id} for iron condor candidates")

        open_positions = _get_open_positions_by_symbol(conn)

        # Use provided symbols or discover from snapshot
        if symbols is None:
            rows = conn.execute(text("""
                SELECT DISTINCT symbol FROM option_quotes
                WHERE snapshot_id = :snapshot_id
                ORDER BY symbol
            """), {"snapshot_id": snapshot_id}).fetchall()
            symbols = [r.symbol for r in rows]

        if not symbols:
            logger.warning(f"No symbols found in snapshot {snapshot_id}")
            return []

        logger.info(f"Scanning {len(symbols)} symbols: {symbols}")

        for symbol in symbols:

            open_strategies = open_positions.get(symbol, [])

            if "IRON_CONDOR" in open_strategies:
                logger.info(
                    f"{symbol}: skipping — already has open IRON_CONDOR position"
                )
                continue

            underlying_price = _get_underlying_price(conn, symbol, snapshot_id)
            if underlying_price is None:
                logger.warning(f"{symbol}: no underlying price — skipping")
                continue

            iv_rank  = _get_iv_rank(conn, symbol)
            contracts = _get_options_for_symbol(conn, symbol, snapshot_id)

            if not contracts:
                logger.warning(
                    f"{symbol}: no contracts in DTE window "
                    f"{HARD_RULES['min_dte']}–{HARD_RULES['max_dte']} — skipping"
                )
                continue

            # Group contracts by expiry
            expiries: dict[tuple, list[dict]] = {}
            for c in contracts:
                key = (str(c["expiry"]), int(c["dte"]))
                expiries.setdefault(key, []).append(c)

            logger.info(
                f"{symbol}: ${underlying_price:.2f} | "
                f"IV rank: {iv_rank if iv_rank else 'N/A'} | "
                f"{len(expiries)} expiries in DTE window"
            )

            # Prefer monthly expirations (3rd Friday) over weekly ones.
            # If at least one monthly expiry exists in the DTE window,
            # drop all weekly expiries for this symbol.
            expiry_keys = sorted(expiries.keys())
            monthly_keys = [(exp, dte) for (exp, dte) in expiry_keys if _is_monthly_expiry(exp)]
            if monthly_keys:
                logger.debug(
                    f"{symbol}: {len(monthly_keys)} monthly expir{'y' if len(monthly_keys) == 1 else 'ies'} "
                    f"found — skipping {len(expiry_keys) - len(monthly_keys)} weekly"
                )
                expiry_keys = monthly_keys

            # Try to build a candidate for each expiry
            symbol_candidates = []
            for (expiry, dte) in expiry_keys:
                expiry_contracts = expiries[(expiry, dte)]
                candidate = _build_candidate_for_expiry(
                    symbol              = symbol,
                    snapshot_id         = snapshot_id,
                    expiry              = expiry,
                    dte                 = dte,
                    contracts           = expiry_contracts,
                    underlying_price    = underlying_price,
                    iv_rank             = iv_rank,
                    existing_strategies = open_strategies if open_strategies else None,
                )
                if candidate:
                    symbol_candidates.append(candidate)

            logger.info(
                f"{symbol}: {len(symbol_candidates)} candidates found "
                f"across {len(expiries)} expiries"
            )
            candidates.extend(symbol_candidates)

        # Generate strangle candidates (additive — runs after condor loop)
        strangle_candidates = generate_strangle_candidates(
            conn            = conn,
            symbols         = symbols,
            snapshot_id     = snapshot_id,
            open_positions  = open_positions,
        )
        candidates.extend(strangle_candidates)

    # Sort by closeness to IDEAL_DTE (38 days — middle of 30–45 sweet spot)
    candidates.sort(key=lambda c: abs(c.dte - IDEAL_DTE))

    logger.info(
        f"Scan complete — {len(candidates)} total candidates "
        f"across {len(symbols)} symbols"
    )

    return candidates


# ── Manual test run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    print("Scanning latest snapshot for iron condor candidates...\n")

    candidates = scan_for_candidates()

    if not candidates:
        print("No candidates found.")
        print("Possible reasons:")
        print("  - No snapshots collected yet (run collector first)")
        print("  - Market was closed when data was collected (empty chains)")
        print("  - No contracts met the delta/credit/width criteria")
    else:
        print(f"Found {len(candidates)} candidates:\n")
        for c in candidates:
            print(
                f"  {c.symbol} | {c.expiry} | DTE={c.dte} | "
                f"P${c.short_put_strike:.0f}/{c.long_put_strike:.0f} "
                f"C${c.short_call_strike:.0f}/{c.long_call_strike:.0f} | "
                f"Credit=${c.net_credit:.2f} | "
                f"MaxLoss=${c.max_loss:.2f} | "
                f"Width=${c.spread_width:.0f} | "
                f"Δput={c.short_put_delta:.3f} Δcall={c.short_call_delta:.3f}"
            )
            if c.scan_notes:
                for note in c.scan_notes:
                    print(f"    ⚠ {note}")
        print(f"\nBest candidate (closest to {IDEAL_DTE} DTE):")
        best = candidates[0]
        print(f"  {best.symbol} {best.expiry} — ${best.net_credit:.2f} credit")