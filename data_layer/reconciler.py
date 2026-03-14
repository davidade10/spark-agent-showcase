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
  Trap 3 — OCC grouping: uses instrument.underlyingSymbol + instrument.expirationDate,
            not OCC string parsing
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

LOG_FILE = Path(__file__).parent.parent / "reconciler.log"

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


# ── Schwab positions parser ───────────────────────────────────────────────────

def _parse_schwab_positions(
    positions: list[dict],
    account_id: str,
    errors: list[str],
) -> list[dict]:
    """
    Extract iron condor positions from the raw Schwab positions list for one
    account.  Returns a list of parsed position dicts, one per condor.

    Schwab returns individual option legs — we group by (underlyingSymbol,
    normalised expirationDate), then identify the four condor legs.

    Traps addressed: 3 (use instrument fields, not OCC parsing), 4 (asymmetric
    qty check), 6 (date normalisation).
    """
    # Filter to OPTION positions only
    option_legs: list[dict] = []
    for pos in positions:
        instr = pos.get("instrument") or {}
        if instr.get("assetType") == "OPTION":
            option_legs.append(pos)

    # Group by (underlying, normalised_expiry)  (Trap 3)
    groups: dict[tuple[str, str], list[dict]] = {}
    for pos in option_legs:
        instr = pos.get("instrument") or {}
        underlying = instr.get("underlyingSymbol") or ""
        exp_raw    = instr.get("expirationDate")
        if not underlying or exp_raw is None:
            logger.warning(
                f"reconciler: skipping leg with missing underlyingSymbol "
                f"or expirationDate — {instr.get('symbol')}"
            )
            continue
        try:
            expiry = _norm_date(exp_raw)   # Trap 6
        except Exception as e:
            logger.warning(f"reconciler: could not normalise date {exp_raw!r} — {e}")
            continue
        groups.setdefault((underlying, expiry), []).append(pos)

    parsed: list[dict] = []

    for (underlying, expiry), legs in groups.items():
        # Separate into long/short puts/calls
        long_puts:   list[dict] = []
        short_puts:  list[dict] = []
        long_calls:  list[dict] = []
        short_calls: list[dict] = []

        for pos in legs:
            instr     = pos.get("instrument") or {}
            put_call  = (instr.get("putCall") or "").upper()   # "PUT" or "CALL"
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

        # A valid iron condor needs exactly one of each leg
        if not (len(long_puts) == len(short_puts) == len(long_calls) == len(short_calls) == 1):
            if len(legs) > 0:
                leg_summary = ", ".join(
                    f"{(p.get('instrument') or {}).get('symbol')} "
                    f"L={p.get('longQuantity')} S={p.get('shortQuantity')}"
                    for p in legs
                )
                logger.warning(
                    f"reconciler: {underlying} {expiry} — "
                    f"incomplete/non-condor group ({len(legs)} legs): {leg_summary}"
                )
            continue

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

        # Extract strikes from instrument.strikePrice (Trap 3)
        def _strike(pos: dict) -> Optional[float]:
            v = (pos.get("instrument") or {}).get("strikePrice")
            return float(v) if v is not None else None

        lp_strike = _strike(lp_pos)
        sp_strike = _strike(sp_pos)
        lc_strike = _strike(lc_pos)
        sc_strike = _strike(sc_pos)

        if any(s is None for s in (lp_strike, sp_strike, lc_strike, sc_strike)):
            # Fall back to OCC symbol parsing for strikes
            import re
            OCC_STRIKE = re.compile(r"(\d{8})$")
            def _occ_strike(pos: dict) -> Optional[float]:
                sym = (pos.get("instrument") or {}).get("symbol") or ""
                m = OCC_STRIKE.search(sym.strip())
                return int(m.group(1)) / 1000.0 if m else None
            lp_strike = lp_strike or _occ_strike(lp_pos)
            sp_strike = sp_strike or _occ_strike(sp_pos)
            lc_strike = lc_strike or _occ_strike(lc_pos)
            sc_strike = sc_strike or _occ_strike(sc_pos)

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

        position_key = (
            f"{underlying}:{expiry}:"
            f"{lp_strike}-{sp_strike}:{sc_strike}-{lc_strike}:{quantity}"
        )

        parsed.append({
            "symbol":            underlying,
            "expiry":            expiry,
            "long_put_strike":   lp_strike,
            "short_put_strike":  sp_strike,
            "short_call_strike": sc_strike,
            "long_call_strike":  lc_strike,
            "quantity":          quantity,
            "fill_credit":       fill_credit,
            "account_id":        account_id,
            "position_key":      position_key,
        })

    return parsed


# ── DB position matcher ───────────────────────────────────────────────────────

def _match_position(
    schwab_pos: dict,
    db_positions: list[dict],
) -> Optional[dict]:
    """
    Find the DB row matching a parsed Schwab position by symbol, expiry,
    and all four strikes.

    Uses math.isclose(abs_tol=0.01) for all strike comparisons (Trap 5).
    Normalises expiry to YYYY-MM-DD before comparing (Trap 6).
    Returns the matching DB dict or None.
    """
    sym    = schwab_pos["symbol"]
    expiry = schwab_pos["expiry"]   # already normalised

    for db in db_positions:
        if db.get("symbol") != sym:
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

        def _close(a, b) -> bool:
            if a is None or b is None:
                return False
            return math.isclose(float(a), float(b), abs_tol=0.01)

        if (
            _close(schwab_pos["long_put_strike"],   db.get("long_put_strike"))
            and _close(schwab_pos["short_put_strike"],  db.get("short_put_strike"))
            and _close(schwab_pos["short_call_strike"], db.get("short_call_strike"))
            and _close(schwab_pos["long_call_strike"],  db.get("long_call_strike"))
        ):
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
    summary: dict[str, list] = {
        "inserted": [],
        "closed":   [],
        "updated":  [],
        "errors":   [],
    }
    now = datetime.now(timezone.utc)

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
        parsed = _parse_schwab_positions(raw_positions, last4, summary["errors"])
        logger.info(
            f"reconcile: account ...{last4} — "
            f"{len(raw_positions)} raw legs → {len(parsed)} iron condors parsed"
        )
        all_schwab_positions.extend(parsed)

    # ── Step 3: load open non-PAPER positions from DB ─────────────────────────
    with engine.connect() as conn:
        db_rows = conn.execute(text("""
            SELECT id, account_id, symbol, expiry,
                   long_put_strike, short_put_strike,
                   short_call_strike, long_call_strike,
                   quantity, fill_credit, status, position_key
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
                try:
                    from datetime import date as _date
                    exp_date = dateutil_parser.parse(sp["expiry"]).date()
                    dte = (exp_date - datetime.now(timezone.utc).date()).days
                except Exception:
                    pass

                conn.execute(text("""
                    INSERT INTO positions (
                        account_id, symbol, expiry, strategy,
                        long_put_strike, short_put_strike,
                        short_call_strike, long_call_strike,
                        quantity, fill_credit,
                        opened_at, status, source, position_key, dte
                    ) VALUES (
                        :account_id, :symbol, :expiry, 'IRON_CONDOR',
                        :long_put_strike, :short_put_strike,
                        :short_call_strike, :long_call_strike,
                        :quantity, :fill_credit,
                        :opened_at, 'open', 'manual', :position_key, :dte
                    )
                    ON CONFLICT (position_key) DO NOTHING
                """), {
                    "account_id":        sp["account_id"],
                    "symbol":            sp["symbol"],
                    "expiry":            sp["expiry"],
                    "long_put_strike":   sp["long_put_strike"],
                    "short_put_strike":  sp["short_put_strike"],
                    "short_call_strike": sp["short_call_strike"],
                    "long_call_strike":  sp["long_call_strike"],
                    "quantity":          sp["quantity"],
                    "fill_credit":       sp["fill_credit"],
                    "opened_at":         now,
                    "position_key":      sp["position_key"],
                    "dte":               dte,
                })
                entry = {
                    "symbol":   sp["symbol"],
                    "expiry":   sp["expiry"],
                    "account":  sp["account_id"],
                    "quantity": sp["quantity"],
                    "fill_credit": sp["fill_credit"],
                }
                summary["inserted"].append(entry)
                logger.info(
                    f"reconcile: INSERTED {sp['symbol']} {sp['expiry']} "
                    f"account=...{sp['account_id']} qty={sp['quantity']} "
                    f"fill_credit={sp['fill_credit']} source=manual"
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
                        SET quantity          = :quantity,
                            long_put_strike   = :long_put_strike,
                            short_put_strike  = :short_put_strike,
                            short_call_strike = :short_call_strike,
                            long_call_strike  = :long_call_strike,
                            last_reconciled_at = :now
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
                    # No discrepancy — just update last_reconciled_at
                    conn.execute(text("""
                        UPDATE positions
                        SET last_reconciled_at = :now
                        WHERE id = :id
                    """), {"now": now, "id": db_match["id"]})

        # DB open positions NOT seen in Schwab → mark closed
        for db in db_positions:
            if db["id"] not in matched_db_ids:
                logger.info(
                    f"reconcile: CLOSING {db['symbol']} {db.get('expiry')} "
                    f"id={db['id']} — not seen in Schwab (expired or manually closed)"
                )
                conn.execute(text("""
                    UPDATE positions
                    SET status      = 'closed',
                        close_reason = 'manual_or_expired',
                        closed_at   = :now,
                        last_reconciled_at = :now
                    WHERE id = :id
                """), {"now": now, "id": db["id"]})
                summary["closed"].append({
                    "id":     db["id"],
                    "symbol": db["symbol"],
                    "expiry": str(db.get("expiry")),
                })

    logger.info(
        f"reconcile complete — "
        f"inserted={len(summary['inserted'])} "
        f"closed={len(summary['closed'])} "
        f"updated={len(summary['updated'])} "
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
        with open(LOG_FILE, "a") as f:
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
