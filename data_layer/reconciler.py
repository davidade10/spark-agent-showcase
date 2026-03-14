"""
data_layer/reconciler.py — Daily Schwab Position Sync + NAV Fetch

Scheduled runs (wire into APScheduler in main.py in a separate step):
  - 9:35 AM ET on trading days   (after open, Schwab positions settle)
  - 4:05 PM ET on trading days   (after close, all fills confirmed)

This module is READ-ONLY from Schwab's perspective. It never places, modifies,
or cancels orders. It only reads positions and balances and syncs them to the DB.

Paper positions (account_id='PAPER') are never reconciled — they are
simulation-only and live exclusively in the DB.

Multi-account: .env has a single SCHWAB_ACCOUNT_HASH for legacy reasons, but
there are TWO live accounts (...5760 Roth IRA and ...8096 Trading). The reconciler
calls get_account_numbers() to discover all hashes dynamically; it never hardcodes
a single hash.

Implementation traps addressed:
  Trap 1 — Multi-account: get_account_numbers() loop, not a single hash
  Trap 2 — Net credit: summed from individual leg averagePrice values
  Trap 3 — OCC grouping: parses instrument.symbol (OCC standard) for root, expiry, type, strike;
            lightweight API does not return underlyingSymbol or expirationDate
  Trap 4 — Asymmetric leg quantities: asserted equal; mismatches go to errors[]
  Trap 5 — Float strike equality: math.isclose(abs_tol=0.01) throughout
  Trap 6 — Date normalization: dateutil.parser → strftime('%Y-%m-%d') everywhere
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dateutil import parser as dateutil_parser
from sqlalchemy import create_engine, text

# Ensure project root is on sys.path when run as __main__
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

PROJECT_ROOT   = Path(__file__).resolve().parent.parent
RECONCILER_LOG = PROJECT_ROOT / "logs" / "reconciler.log"
RECONCILER_LOG.parent.mkdir(parents=True, exist_ok=True)

# ── Date normalisation helper ─────────────────────────────────────────────────

def _norm_date(val) -> str:
    """
    Normalise any Schwab date/datetime value to a 'YYYY-MM-DD' string.

    Schwab returns expirationDate in varying formats
    (e.g. '2026-04-17T00:00:00+0000', '2026-04-17', epoch ms).
    Using dateutil.parser handles all of them. (Trap 6)
    """
    if val is None:
        raise ValueError("Cannot normalise None date")
    if isinstance(val, (int, float)):
        # epoch milliseconds
        from datetime import datetime as _dt
        return _dt.utcfromtimestamp(val / 1000).strftime("%Y-%m-%d")
    s = str(val).strip()
    return dateutil_parser.parse(s).strftime("%Y-%m-%d")


# ── Leg avg-price extractor ───────────────────────────────────────────────────

def _leg_avg_price(pos: dict, qty_signed: int) -> Optional[float]:
    """
    Extract the average fill price for a single option leg from a Schwab
    position object.  Schwab stores long and short averages under different
    keys depending on the account type — try all known variants. (Trap 2)
    """
    if qty_signed < 0:
        for key in ("averageShortPrice", "taxLotAverageShortPrice", "averagePrice"):
            v = pos.get(key)
            if isinstance(v, (int, float)) and float(v) > 0:
                return float(v)
    else:
        for key in ("averageLongPrice", "taxLotAverageLongPrice", "averagePrice"):
            v = pos.get(key)
            if isinstance(v, (int, float)) and float(v) > 0:
                return float(v)
    return None


# ── OCC symbol parser ─────────────────────────────────────────────────────────

def _parse_occ_symbol(occ_string: str) -> dict:
    """
    Parse an OCC option symbol by character position (OCC standard).

    Layout: [0:6] root (strip trailing spaces), [6:12] YYMMDD, [12:13] C|P, [13:21] strike*1000.
    Returns dict: root (str), expiry (YYYY-MM-DD), option_type ('C' or 'P'), strike (float).
    Raises ValueError if the string is too short or invalid.
    """
    s = (occ_string or "").strip()
    if len(s) < 21:
        raise ValueError(f"OCC symbol too short: {occ_string!r}")
    root = s[0:6].rstrip()
    yy_mm_dd = s[6:12]
    option_type = s[12:13].upper()
    strike_str = s[13:21]
    if option_type not in ("C", "P"):
        raise ValueError(f"Invalid OCC option type: {option_type!r}")
    try:
        year = 2000 + int(yy_mm_dd[0:2])
        month = int(yy_mm_dd[2:4])
        day = int(yy_mm_dd[4:6])
        expiry = f"{year:04d}-{month:02d}-{day:02d}"
    except (ValueError, IndexError) as e:
        raise ValueError(f"Invalid OCC expiry {yy_mm_dd!r}") from e
    try:
        strike = int(strike_str) / 1000.0
    except ValueError as e:
        raise ValueError(f"Invalid OCC strike {strike_str!r}") from e
    return {"root": root, "expiry": expiry, "option_type": option_type, "strike": strike}


# ── Schwab positions parser ───────────────────────────────────────────────────

def _parse_schwab_positions(
    positions: list[dict],
    account_id: str,
    errors: list[str],
) -> tuple[list[dict], list[dict]]:
    """
    Extract positions from raw Schwab data for one account.

    Returns (condors, non_condors) — two separate lists so the caller can
    distinguish strategies and update the parser health check accordingly.

    Iron condor path is unchanged from before (byte-for-byte identical logic).
    Non-condor path handles EQUITY, single-leg options, verticals, and other
    multi-leg structures that are not 4-leg iron condors.

    Schwab's lightweight instrument block does not include underlyingSymbol or
    expirationDate; we parse the OCC symbol (instrument.symbol) to get root,
    expiry, put/call type, and strike, then group by (root, expiry).
    """
    # ── 0. Handle EQUITY positions first (shares from assignment, etc.) ───────
    non_condors: list[dict] = []

    for pos in positions:
        instr = pos.get("instrument") or {}
        if instr.get("assetType") != "EQUITY":
            continue
        symbol   = instr.get("symbol") or instr.get("cusip") or "UNKNOWN"
        long_qty  = int(pos.get("longQuantity")  or 0)
        short_qty = int(pos.get("shortQuantity") or 0)
        quantity  = long_qty - short_qty
        avg_price = None
        for key in ("averageLongPrice", "averagePrice", "taxLotAverageLongPrice"):
            v = pos.get(key)
            if isinstance(v, (int, float)) and float(v) > 0:
                avg_price = float(v)
                break
        position_key = f"{symbol}:EQUITY"
        leg_detail = [{"symbol": symbol, "qty": quantity, "avg_price": avg_price}]
        non_condors.append({
            "symbol":             symbol,
            "expiry":             None,
            "strategy":           "EQUITY",
            "quantity":           abs(quantity),
            "fill_credit":        avg_price,
            "account_id":         account_id,
            "position_key":       position_key,
            "legs_json":          json.dumps(leg_detail),
            "long_put_strike":    None,
            "short_put_strike":   None,
            "short_call_strike":  None,
            "long_call_strike":   None,
            "legs":               1,     # leg count, used by health-check caller
        })
        logger.info(
            f"reconciler: EQUITY {symbol} qty={quantity} "
            f"avg_price={avg_price} account=...{account_id}"
        )

    # ── 1. Filter to OPTION positions only ────────────────────────────────────
    option_legs: list[dict] = []
    for pos in positions:
        instr = pos.get("instrument") or {}
        if instr.get("assetType") == "OPTION":
            option_legs.append(pos)

    # ── 2. Group by (underlying, expiry) using OCC symbol parsing ─────────────
    groups: dict[tuple[str, str], list[dict]] = {}
    for pos in option_legs:
        instr = pos.get("instrument") or {}
        occ_raw = instr.get("symbol")
        if not occ_raw:
            logger.warning("reconciler: skipping leg with no instrument.symbol")
            continue
        try:
            parsed_occ = _parse_occ_symbol(occ_raw)
        except ValueError as e:
            logger.warning(f"reconciler: skipping leg — OCC parse failed for {occ_raw!r}: {e}")
            continue
        underlying = parsed_occ["root"]
        expiry = parsed_occ["expiry"]
        pos["_occ"] = parsed_occ
        groups.setdefault((underlying, expiry), []).append(pos)

    condors: list[dict] = []

    for (underlying, expiry), legs in groups.items():
        # ── Classify legs ─────────────────────────────────────────────────────
        long_puts:   list[dict] = []
        short_puts:  list[dict] = []
        long_calls:  list[dict] = []
        short_calls: list[dict] = []

        for pos in legs:
            occ = pos.get("_occ") or {}
            put_call  = "PUT" if occ.get("option_type") == "P" else "CALL"
            long_qty  = int(pos.get("longQuantity")  or 0)
            short_qty = int(pos.get("shortQuantity") or 0)

            if put_call == "PUT":
                if long_qty > 0:
                    long_puts.append(pos)
                elif short_qty > 0:
                    short_puts.append(pos)
            elif put_call == "CALL":
                if long_qty > 0:
                    long_calls.append(pos)
                elif short_qty > 0:
                    short_calls.append(pos)

        # ── Iron condor path (UNCHANGED) ─────────────────────────────────────
        if len(long_puts) == len(short_puts) == len(long_calls) == len(short_calls) == 1:
            lp_pos = long_puts[0]
            sp_pos = short_puts[0]
            lc_pos = long_calls[0]
            sc_pos = short_calls[0]

            lp_qty = int(lp_pos.get("longQuantity")  or 0)
            sp_qty = int(sp_pos.get("shortQuantity") or 0)
            lc_qty = int(lc_pos.get("longQuantity")  or 0)
            sc_qty = int(sc_pos.get("shortQuantity") or 0)

            # Trap 4 — assert all leg quantities are identical
            if not (lp_qty == sp_qty == lc_qty == sc_qty) or lp_qty == 0:
                leg_detail = (
                    f"long_put={lp_qty} short_put={sp_qty} "
                    f"long_call={lc_qty} short_call={sc_qty}"
                )
                msg = (
                    f"reconciler: {underlying} {expiry} — "
                    f"asymmetric leg quantities ({leg_detail}); "
                    f"skipping — investigate naked risk"
                )
                logger.warning(msg)
                errors.append(msg)
                continue

            quantity = lp_qty

            def _strike(pos: dict) -> Optional[float]:
                occ = pos.get("_occ")
                if occ and "strike" in occ:
                    return occ["strike"]
                v = (pos.get("instrument") or {}).get("strikePrice")
                return float(v) if v is not None else None

            lp_strike = _strike(lp_pos)
            sp_strike = _strike(sp_pos)
            lc_strike = _strike(lc_pos)
            sc_strike = _strike(sc_pos)

            if any(s is None for s in (lp_strike, sp_strike, lc_strike, sc_strike)):
                msg = (
                    f"reconciler: {underlying} {expiry} — "
                    "could not determine strikes; skipping"
                )
                logger.warning(msg)
                errors.append(msg)
                continue

            # Trap 2 — compute fill_credit from individual leg avg prices
            lp_avg = _leg_avg_price(lp_pos, +1)
            sp_avg = _leg_avg_price(sp_pos, -1)
            lc_avg = _leg_avg_price(lc_pos, +1)
            sc_avg = _leg_avg_price(sc_pos, -1)

            if all(x is not None for x in (lp_avg, sp_avg, lc_avg, sc_avg)):
                fill_credit = round(sp_avg + sc_avg - lp_avg - lc_avg, 4)
            else:
                fill_credit = None
                logger.warning(
                    f"reconciler: {underlying} {expiry} — "
                    f"could not compute fill_credit (missing leg avg prices); "
                    f"inserting with fill_credit=NULL"
                )

            # Iron condor position_key — PRESERVED EXACTLY (backward compat)
            position_key = (
                f"{underlying}:{expiry}:"
                f"{lp_strike}-{sp_strike}:{sc_strike}-{lc_strike}:{quantity}"
            )

            condors.append({
                "symbol":            underlying,
                "expiry":            expiry,
                "strategy":          "IRON_CONDOR",
                "long_put_strike":   lp_strike,
                "short_put_strike":  sp_strike,
                "short_call_strike": sc_strike,
                "long_call_strike":  lc_strike,
                "quantity":          quantity,
                "fill_credit":       fill_credit,
                "account_id":        account_id,
                "position_key":      position_key,
                "legs_json":         None,
                "legs":              4,  # leg count for health-check
            })
            continue  # handled as condor — skip non-condor path

        # ── Non-condor option path ─────────────────────────────────────────────
        all_legs = long_puts + short_puts + long_calls + short_calls
        n_legs   = len(all_legs)

        def _occ_strike(pos: dict) -> Optional[float]:
            occ = pos.get("_occ")
            if occ and "strike" in occ:
                return occ["strike"]
            v = (pos.get("instrument") or {}).get("strikePrice")
            return float(v) if v is not None else None

        # Build leg details for legs_json
        leg_details = []
        for pos in all_legs:
            occ = pos.get("_occ") or {}
            leg_details.append({
                "occ_symbol": (pos.get("instrument") or {}).get("symbol"),
                "option_type": occ.get("option_type"),
                "strike":      occ.get("strike"),
                "long_qty":    int(pos.get("longQuantity")  or 0),
                "short_qty":   int(pos.get("shortQuantity") or 0),
            })

        # Determine strategy
        strategy = "UNKNOWN"
        if n_legs == 1:
            pos = all_legs[0]
            occ = pos.get("_occ") or {}
            long_qty  = int(pos.get("longQuantity")  or 0)
            short_qty = int(pos.get("shortQuantity") or 0)
            if short_qty > 0:
                strategy = "SHORT_OPTION"
            elif long_qty > 0:
                strategy = "LONG_OPTION"
        elif n_legs == 2 and len(all_legs) == 2:
            has_long  = len(long_puts) + len(long_calls) == 1
            has_short = len(short_puts) + len(short_calls) == 1
            if has_long and has_short:
                # Same type → vertical spread; different types → strangle/straddle
                same_type = (
                    (len(long_puts) == 1 and len(short_puts) == 1) or
                    (len(long_calls) == 1 and len(short_calls) == 1)
                )
                if same_type:
                    strategy = "VERTICAL_SPREAD"
                else:
                    strikes = [_occ_strike(p) for p in all_legs]
                    if all(s is not None for s in strikes) and math.isclose(
                        float(strikes[0]), float(strikes[1]), abs_tol=0.01  # type: ignore[arg-type]
                    ):
                        strategy = "STRADDLE"
                    else:
                        strategy = "STRANGLE"

        # Quantity: use the first leg's non-zero qty
        quantity = 0
        fill_credit = None
        for pos in all_legs:
            lq = int(pos.get("longQuantity")  or 0)
            sq = int(pos.get("shortQuantity") or 0)
            if lq > 0:
                quantity = lq
            elif sq > 0:
                quantity = sq
            avg = _leg_avg_price(pos, -sq if sq > 0 else lq)
            if avg is not None and fill_credit is None:
                fill_credit = avg if sq > 0 else -avg  # credit positive for shorts

        # Populate only the available strike columns
        sc_strike = _occ_strike(short_calls[0]) if short_calls else None
        sp_strike = _occ_strike(short_puts[0])  if short_puts  else None
        lc_strike = _occ_strike(long_calls[0])  if long_calls  else None
        lp_strike = _occ_strike(long_puts[0])   if long_puts   else None

        # Position key for non-condors — includes strategy to avoid collision
        strike_parts = [
            str(s) for s in (lp_strike, sp_strike, sc_strike, lc_strike)
            if s is not None
        ]
        position_key = (
            f"{underlying}_{expiry}_{strategy}_"
            + ("-".join(strike_parts) if strike_parts else "nostrike")
        )

        non_condors.append({
            "symbol":            underlying,
            "expiry":            expiry,
            "strategy":          strategy,
            "quantity":          quantity,
            "fill_credit":       fill_credit,
            "account_id":        account_id,
            "position_key":      position_key,
            "legs_json":         json.dumps(leg_details),
            "long_put_strike":   lp_strike,
            "short_put_strike":  sp_strike,
            "short_call_strike": sc_strike,
            "long_call_strike":  lc_strike,
            "legs":              n_legs,  # leg count for health-check
        })
        logger.info(
            f"reconciler: non-condor {strategy} {underlying} {expiry} "
            f"qty={quantity} account=...{account_id}"
        )

    return condors, non_condors


# ── DB position matcher ───────────────────────────────────────────────────────

def _match_position(
    schwab_pos: dict,
    db_positions: list[dict],
) -> Optional[dict]:
    """
    Find the DB row matching a parsed Schwab position.

    Matching rules by strategy:
      IRON_CONDOR    — symbol + expiry + all four strikes (unchanged, Traps 5+6)
      VERTICAL_SPREAD/STRANGLE/STRADDLE — symbol + expiry + strategy + non-null strikes
      SHORT_OPTION/LONG_OPTION — symbol + expiry + strategy + the one populated strike
      EQUITY         — symbol + strategy only (no expiry or strikes)

    Uses math.isclose(abs_tol=0.01) for all strike comparisons (Trap 5).
    Normalises expiry to YYYY-MM-DD before comparing (Trap 6).
    Returns the matching DB dict or None.
    """
    def _close(a, b) -> bool:
        if a is None or b is None:
            return False
        return math.isclose(float(a), float(b), abs_tol=0.01)

    sym      = schwab_pos["symbol"]
    strategy = schwab_pos.get("strategy", "IRON_CONDOR")

    # EQUITY: match by symbol + strategy only
    if strategy == "EQUITY":
        for db in db_positions:
            if db.get("symbol") == sym and db.get("strategy") == "EQUITY":
                return db
        return None

    expiry = schwab_pos.get("expiry")  # already normalised

    for db in db_positions:
        if db.get("symbol") != sym:
            continue

        # Strategy must match for non-condor rows (condor rows may have NULL strategy
        # in legacy data, so only enforce for non-condors)
        db_strategy = db.get("strategy") or "IRON_CONDOR"
        if strategy != "IRON_CONDOR" and db_strategy != strategy:
            continue

        db_expiry = db.get("expiry")
        if db_expiry is None:
            continue
        try:
            db_expiry_str = _norm_date(db_expiry)
        except Exception:
            continue
        if db_expiry_str != expiry:
            continue

        if strategy == "IRON_CONDOR":
            # Original condor matching — all four strikes must match
            if (
                _close(schwab_pos["long_put_strike"],   db.get("long_put_strike"))
                and _close(schwab_pos["short_put_strike"],  db.get("short_put_strike"))
                and _close(schwab_pos["short_call_strike"], db.get("short_call_strike"))
                and _close(schwab_pos["long_call_strike"],  db.get("long_call_strike"))
            ):
                return db
        else:
            # Non-condor: match on whichever strike columns are non-null in the
            # Schwab position; skip columns where Schwab has None
            strike_cols = (
                "long_put_strike", "short_put_strike",
                "short_call_strike", "long_call_strike",
            )
            all_match = True
            any_strike_checked = False
            for col in strike_cols:
                sw_val = schwab_pos.get(col)
                if sw_val is None:
                    continue  # not populated for this strategy — skip
                any_strike_checked = True
                if not _close(sw_val, db.get(col)):
                    all_match = False
                    break
            # For strategies where no strikes are populated (shouldn't happen
            # for options), fall through without matching to avoid false positives
            if all_match and (any_strike_checked or strategy == "EQUITY"):
                return db

    return None


# ── Core reconciliation ───────────────────────────────────────────────────────

def reconcile(engine, schwab_client) -> dict:
    """
    Main reconciliation function.

    1. Calls get_account_numbers() to discover all account hashes + masked numbers.
       (Trap 1 — never assumes a single hash)
    2. For each live account (skips PAPER), fetches positions from Schwab.
    3. Queries all open non-PAPER positions from the DB.
    4. Three-way comparison:
         in Schwab, not DB  → INSERT with source='manual'
         in DB, not Schwab  → UPDATE status='closed', close_reason='manual_or_expired'
         in both, qty/strike differ → UPDATE DB row to match, log discrepancy
    5. Returns {"inserted": [...], "closed": [...], "updated": [...], "errors": [...]}
    """
    summary: dict = {
        "inserted":     [],
        "closed":       [],
        "updated":      [],
        "errors":       [],
        "skipped_legs": 0,
        "run_id":       None,
    }
    now = datetime.now(timezone.utc)

    # ── Run counter ───────────────────────────────────────────────────────────
    run_id: Optional[int] = None
    try:
        with engine.begin() as _rc_conn:
            row = _rc_conn.execute(text(
                "SELECT value FROM reconciler_state WHERE key = 'run_count'"
            )).fetchone()
            run_id = (int(row[0]) if row else 0) + 1
            _rc_conn.execute(text("""
                INSERT INTO reconciler_state (key, value) VALUES ('run_count', :v)
                ON CONFLICT (key) DO UPDATE SET value = :v
            """), {"v": str(run_id)})
        summary["run_id"] = run_id
        logger.info(f"reconcile: starting run_id={run_id}")
    except Exception as e:
        logger.warning(f"reconcile: could not increment run_count — {e}")

    # ── Step 1: discover all account hashes ───────────────────────────────────
    try:
        acct_numbers_resp = schwab_client.get_account_numbers()
        acct_numbers_resp.raise_for_status()
        acct_entries = acct_numbers_resp.json()
    except Exception as e:
        msg = f"reconcile: get_account_numbers() failed — {e}"
        logger.error(msg)
        summary["errors"].append(msg)
        return summary

    # acct_entries is a list of {"accountNumber": "...5760", "hashValue": "abc..."}
    account_map: dict[str, str] = {}   # last-4 → hashValue
    for entry in acct_entries:
        acct_num  = str(entry.get("accountNumber") or "")
        hash_val  = str(entry.get("hashValue") or "")
        last4     = acct_num[-4:] if len(acct_num) >= 4 else acct_num
        account_map[last4] = hash_val
        logger.info(f"reconcile: discovered account ...{last4} hash={hash_val[:8]}…")

    if not account_map:
        msg = "reconcile: no accounts returned from get_account_numbers()"
        logger.error(msg)
        summary["errors"].append(msg)
        return summary

    # ── Step 2: fetch Schwab positions for all live accounts ──────────────────
    all_schwab_positions: list[dict] = []
    total_legs_received:  int = 0
    total_condors_parsed: int = 0

    for last4, hash_val in account_map.items():
        try:
            resp = schwab_client.get_account(
                hash_val,
                fields=schwab_client.Account.Fields.POSITIONS,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            msg = f"reconcile: get_account({last4}) failed — {e}"
            logger.error(msg)
            summary["errors"].append(msg)
            continue

        raw_positions = (
            data.get("securitiesAccount", {}).get("positions") or []
        )
        # Count OPTION legs only — equity positions excluded from health check
        option_legs = sum(
            1 for p in raw_positions
            if (p.get("instrument") or {}).get("assetType") == "OPTION"
        )
        total_legs_received += option_legs

        condors, non_condors = _parse_schwab_positions(
            raw_positions, last4, summary["errors"]
        )
        total_condors_parsed += len(condors)
        logger.info(
            f"reconcile: account ...{last4} — "
            f"{option_legs} option legs → {len(condors)} condors, "
            f"{len(non_condors)} non-condor positions parsed"
        )
        all_schwab_positions.extend(condors)
        all_schwab_positions.extend(non_condors)

    # ── Parser health check — block closures if parser looks broken ───────────
    # Only count OPTION legs as "recognized" when they become condors or
    # non-condor option strategies.  EQUITY positions never were option legs.
    recognized_option_legs = total_condors_parsed * 4 + sum(
        p.get("legs", 0)
        for p in all_schwab_positions
        if p.get("strategy") not in ("IRON_CONDOR", "EQUITY")
    )
    skipped_legs = max(0, total_legs_received - recognized_option_legs)
    summary["skipped_legs"] = skipped_legs
    closures_blocked = False

    if total_legs_received >= 4:
        recognized_total = total_legs_received - skipped_legs
        parse_ratio = recognized_total / total_legs_received

        if recognized_total == 0:
            msg = (
                f"Parser produced 0 recognized positions from {total_legs_received} "
                "option legs — closure writes BLOCKED for safety. Investigate parser."
            )
            logger.critical(msg)
            summary["errors"].append(msg)
            closures_blocked = True
        elif parse_ratio < 0.5:
            msg = (
                f"Parser health WARNING: recognized {recognized_total} of "
                f"{total_legs_received} option legs (ratio={parse_ratio:.2f} < 0.5) — "
                "closure writes BLOCKED for safety."
            )
            logger.warning(msg)
            summary["errors"].append(msg)
            closures_blocked = True

    # ── Step 3: load open non-PAPER positions from DB ─────────────────────────
    with engine.connect() as conn:
        db_rows = conn.execute(text("""
            SELECT id, account_id, symbol, expiry,
                   long_put_strike, short_put_strike,
                   short_call_strike, long_call_strike,
                   quantity, fill_credit, status, position_key,
                   COALESCE(closure_strikes, 0) AS closure_strikes
            FROM positions
            WHERE status = 'open'
              AND account_id != 'PAPER'
        """)).fetchall()

    db_positions: list[dict] = [dict(r._mapping) for r in db_rows]

    # ── Step 4: three-way reconcile ───────────────────────────────────────────
    matched_db_ids: set[int] = set()

    with engine.begin() as conn:
        for sp in all_schwab_positions:
            db_match = _match_position(sp, db_positions)

            if db_match is None:
                # In Schwab but not in DB → insert as source='manual'
                dte = None
                sp_expiry = sp.get("expiry")
                if sp_expiry:
                    try:
                        exp_date = dateutil_parser.parse(sp_expiry).date()
                        dte = (exp_date - datetime.now(timezone.utc).date()).days
                    except Exception:
                        pass

                conn.execute(text("""
                    INSERT INTO positions (
                        account_id, symbol, expiry, strategy,
                        long_put_strike, short_put_strike,
                        short_call_strike, long_call_strike,
                        quantity, fill_credit, legs_json,
                        opened_at, status, source, position_key, dte
                    ) VALUES (
                        :account_id, :symbol, :expiry, :strategy,
                        :long_put_strike, :short_put_strike,
                        :short_call_strike, :long_call_strike,
                        :quantity, :fill_credit, :legs_json,
                        :opened_at, 'open', 'manual', :position_key, :dte
                    )
                    ON CONFLICT (position_key) DO NOTHING
                """), {
                    "account_id":        sp["account_id"],
                    "symbol":            sp["symbol"],
                    "expiry":            sp.get("expiry"),
                    "strategy":          sp.get("strategy", "IRON_CONDOR"),
                    "long_put_strike":   sp.get("long_put_strike"),
                    "short_put_strike":  sp.get("short_put_strike"),
                    "short_call_strike": sp.get("short_call_strike"),
                    "long_call_strike":  sp.get("long_call_strike"),
                    "quantity":          sp["quantity"],
                    "fill_credit":       sp.get("fill_credit"),
                    "legs_json":         sp.get("legs_json"),
                    "opened_at":         now,
                    "position_key":      sp["position_key"],
                    "dte":               dte,
                })
                entry = {
                    "symbol":    sp["symbol"],
                    "expiry":    sp.get("expiry"),
                    "strategy":  sp.get("strategy", "IRON_CONDOR"),
                    "account":   sp["account_id"],
                    "quantity":  sp["quantity"],
                    "fill_credit": sp.get("fill_credit"),
                }
                summary["inserted"].append(entry)
                logger.info(
                    f"reconcile: INSERTED {sp['symbol']} {sp.get('expiry', 'n/a')} "
                    f"strategy={sp.get('strategy','IRON_CONDOR')} "
                    f"account=...{sp['account_id']} qty={sp['quantity']} "
                    f"fill_credit={sp.get('fill_credit')} source=manual"
                )

            else:
                # Matched — check for discrepancies
                matched_db_ids.add(db_match["id"])
                changes: dict[str, tuple] = {}

                if db_match.get("quantity") != sp["quantity"]:
                    changes["quantity"] = (db_match.get("quantity"), sp["quantity"])

                def _strike_differs(db_val, sw_val) -> bool:
                    if db_val is None or sw_val is None:
                        return db_val != sw_val
                    return not math.isclose(float(db_val), float(sw_val), abs_tol=0.01)

                for strike_col, sw_val in (
                    ("long_put_strike",   sp["long_put_strike"]),
                    ("short_put_strike",  sp["short_put_strike"]),
                    ("short_call_strike", sp["short_call_strike"]),
                    ("long_call_strike",  sp["long_call_strike"]),
                ):
                    if _strike_differs(db_match.get(strike_col), sw_val):
                        changes[strike_col] = (db_match.get(strike_col), sw_val)

                if changes:
                    logger.warning(
                        f"reconcile: UPDATING {sp['symbol']} {sp['expiry']} "
                        f"id={db_match['id']} — discrepancies: {changes}"
                    )
                    conn.execute(text("""
                        UPDATE positions
                        SET quantity             = :quantity,
                            long_put_strike      = :long_put_strike,
                            short_put_strike     = :short_put_strike,
                            short_call_strike    = :short_call_strike,
                            long_call_strike     = :long_call_strike,
                            last_reconciled_at   = :now,
                            closure_strikes      = 0,
                            last_seen_in_schwab  = :now
                        WHERE id = :id
                    """), {
                        "quantity":          sp["quantity"],
                        "long_put_strike":   sp["long_put_strike"],
                        "short_put_strike":  sp["short_put_strike"],
                        "short_call_strike": sp["short_call_strike"],
                        "long_call_strike":  sp["long_call_strike"],
                        "now":               now,
                        "id":                db_match["id"],
                    })
                    summary["updated"].append({
                        "id":       db_match["id"],
                        "symbol":   sp["symbol"],
                        "expiry":   sp["expiry"],
                        "changes":  {k: {"before": v[0], "after": v[1]} for k, v in changes.items()},
                    })
                else:
                    # No discrepancy — reset strike counter, update timestamps
                    conn.execute(text("""
                        UPDATE positions
                        SET last_reconciled_at  = :now,
                            closure_strikes     = 0,
                            last_seen_in_schwab = :now
                        WHERE id = :id
                    """), {"now": now, "id": db_match["id"]})

        # DB open positions NOT seen in Schwab → 3-strike system before closing
        for db in db_positions:
            if db["id"] not in matched_db_ids:
                sym    = db["symbol"]
                expiry = str(db.get("expiry"))

                if closures_blocked:
                    logger.info(
                        f"reconcile: SKIPPING potential closure of {sym} {expiry} "
                        f"id={db['id']} — closures blocked by parser health check"
                    )
                    if summary["skipped_legs"] > 0:
                        logger.warning(
                            f"WARNING: {summary['skipped_legs']} legs were unparseable — "
                            f"closures may be due to parser failure, not actual position absence"
                        )
                    continue

                current_strikes = int(db.get("closure_strikes") or 0)
                new_strikes = current_strikes + 1

                if new_strikes < 3:
                    # Not enough consecutive absences — record strike, do NOT close
                    conn.execute(text("""
                        UPDATE positions
                        SET closure_strikes    = :strikes,
                            last_reconciled_at = :now
                        WHERE id = :id
                    """), {"strikes": new_strikes, "now": now, "id": db["id"]})
                    logger.info(
                        f"reconcile: Strike {new_strikes}/3 for {sym} {expiry} "
                        f"id={db['id']} — not seen in Schwab "
                        f"(will close after 3 consecutive absences)"
                    )
                else:
                    # 3 consecutive absences confirmed — close
                    logger.info(
                        f"reconcile: 3 consecutive absences confirmed — "
                        f"closing {sym} {expiry} id={db['id']}"
                    )
                    conn.execute(text("""
                        UPDATE positions
                        SET status             = 'closed',
                            close_reason       = 'manual_or_expired',
                            closed_at          = :now,
                            last_reconciled_at = :now
                        WHERE id = :id
                    """), {"now": now, "id": db["id"]})
                    summary["closed"].append({
                        "id":     db["id"],
                        "symbol": sym,
                        "expiry": expiry,
                    })

    logger.info(
        f"reconcile complete — "
        f"run_id={run_id} "
        f"inserted={len(summary['inserted'])} "
        f"closed={len(summary['closed'])} "
        f"updated={len(summary['updated'])} "
        f"skipped_legs={summary['skipped_legs']} "
        f"errors={len(summary['errors'])}"
    )
    return summary


# ── NAV fetch ─────────────────────────────────────────────────────────────────

def reconcile_nav(engine, schwab_client) -> dict:
    """
    Fetches current account balances for all linked Schwab accounts.

    Returns:
        {
            "accounts": {
                "5760": 6621.12,
                "8096": 8192.34,
            },
            "combined_live_nav": 14813.46,
        }

    Will eventually replace the placeholder NAVs in approval_ui/api.py's
    /accounts endpoint and the LIVE_ACCOUNT_NAV in config.
    """
    try:
        acct_numbers_resp = schwab_client.get_account_numbers()
        acct_numbers_resp.raise_for_status()
        acct_entries = acct_numbers_resp.json()
    except Exception as e:
        logger.error(f"reconcile_nav: get_account_numbers() failed — {e}")
        return {"accounts": {}, "combined_live_nav": 0.0, "error": str(e)}

    navs: dict[str, float] = {}

    for entry in acct_entries:
        acct_num = str(entry.get("accountNumber") or "")
        hash_val = str(entry.get("hashValue") or "")
        last4    = acct_num[-4:] if len(acct_num) >= 4 else acct_num

        try:
            resp = schwab_client.get_account(hash_val)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"reconcile_nav: get_account({last4}) failed — {e}")
            continue

        balances = (
            data.get("securitiesAccount", {})
                .get("currentBalances", {})
        )
        nav = float(balances.get("liquidationValue") or 0.0)
        navs[last4] = nav
        logger.info(f"reconcile_nav: account ...{last4} NAV=${nav:,.2f}")

    combined = round(sum(navs.values()), 2)
    logger.info(f"reconcile_nav: combined live NAV=${combined:,.2f}")

    return {
        "accounts":         navs,
        "combined_live_nav": combined,
    }


# ── Scheduled entry point ─────────────────────────────────────────────────────

def run_scheduled_reconciliation() -> None:
    """
    Entry point for scheduled runs (APScheduler / cron).

    Wiring into main.py APScheduler is done in a separate step.
    Intended schedule: 9:35 AM ET and 4:05 PM ET on trading days.

    Initialises the Schwab client via data_layer/provider.py, runs both
    reconcile() and reconcile_nav(), logs the full summary, and appends
    a timestamped JSON record to reconciler.log.
    """
    from data_layer.provider import get_schwab_client
    from execution.order_state import migrate_orders_schema

    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    engine = create_engine(DB_URL)
    migrate_orders_schema(engine)

    try:
        client = get_schwab_client()
    except Exception as e:
        logger.error(f"run_scheduled_reconciliation: could not init Schwab client — {e}")
        return

    ts = datetime.now(timezone.utc).isoformat()

    pos_summary = reconcile(engine, client)
    nav_summary = reconcile_nav(engine, client)

    full_summary = {
        "ts":          ts,
        "positions":   pos_summary,
        "nav":         nav_summary,
    }

    logger.info(f"Reconciliation summary: {json.dumps(full_summary, default=str)}")

    # Append to reconciler.log
    try:
        with open(RECONCILER_LOG, "a") as f:
            f.write(json.dumps(full_summary, default=str) + "\n")
    except Exception as e:
        logger.warning(f"Could not write to reconciler.log — {e}")


# ── CLI entry point ───────────────────────────────────────────────────────────

def _cli() -> None:
    ap = argparse.ArgumentParser(
        description="Run the Schwab position reconciler on demand."
    )
    ap.add_argument(
        "--now",
        action  = "store_true",
        help    = "Run reconciliation immediately and exit.",
    )
    args = ap.parse_args()

    if args.now:
        run_scheduled_reconciliation()
    else:
        ap.print_help()


if __name__ == "__main__":
    _cli()
