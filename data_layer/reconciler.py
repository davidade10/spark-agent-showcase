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

# Asset types that Schwab classifies as share-based holdings (strategy = "EQUITY").
# ETF / closed-end fund / mutual fund / collective investment behave identically
# to plain EQUITY for reconciler purposes — they have a qty and an avg cost basis.
EQUITY_LIKE_TYPES = {"EQUITY", "ETF", "CLOSED_END_FUND", "MUTUAL_FUND", "COLLECTIVE_INVESTMENT"}

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


def _index_active_orders_for_partials(conn) -> dict[tuple[str, str, str], list[dict]]:
    """
    Build an index of active multi-leg option orders keyed by
    (account_id, symbol, expiry_iso).

    Each entry contains:
      {
        "id": order_id,
        "occs": set of OCC leg symbols,
        "quantity": order-level quantity,
      }
    """
    rows = conn.execute(text("""
        SELECT id, account_id, symbol, order_payload
        FROM orders
        WHERE status NOT IN ('filled', 'rejected', 'cancelled')
          AND source != 'live_dry_run'
          AND created_at > (NOW() - INTERVAL '24 hours')
    """)).fetchall()

    index: dict[tuple[str, str, str], list[dict]] = {}
    for r in rows:
        try:
            payload_raw = r.order_payload
            payload = (
                payload_raw if isinstance(payload_raw, dict)
                else json.loads(payload_raw or "{}")
            )
        except Exception:
            continue
        legs = payload.get("orderLegCollection") or []
        if not isinstance(legs, list) or not legs:
            continue
        occs: set[str] = set()
        expiry_iso: Optional[str] = None
        for leg in legs:
            inst = (leg or {}).get("instrument") or {}
            occ_sym = inst.get("symbol")
            if not occ_sym:
                continue
            occs.add(occ_sym)
            if expiry_iso is None:
                try:
                    parsed = _parse_occ_symbol(str(occ_sym))
                    expiry_iso = parsed["expiry"]
                except Exception:
                    continue
        if not occs or not expiry_iso:
            continue
        key = (str(r.account_id or ""), str(r.symbol or ""), expiry_iso)
        entry = {
            "id":       int(r.id),
            "occs":     occs,
            "quantity": int(payload.get("quantity") or 0),
        }
        index.setdefault(key, []).append(entry)
    return index


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
        if instr.get("assetType") not in EQUITY_LIKE_TYPES:
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
        position_key = f"{symbol}:EQUITY:{account_id}"
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
    # assetType == "OPTION" is the only type we parse as option legs.
    # EQUITY_LIKE_TYPES (ETF, CLOSED_END_FUND, etc.) are share-based and already
    # handled above — they must NOT enter the option-leg path.
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
        # Targeted raw payload audit: IWM rows only (pre-grouping).
        # Keep concise; helps diagnose duplicate rows, stale/closed-leg fields,
        # and zero-quantity ghosts.
        try:
            root = str(occ_raw)[0:6].rstrip()
        except Exception:
            root = ""
        if root == "IWM":
            long_q = pos.get("longQuantity")
            short_q = pos.get("shortQuantity")
            avg_p = pos.get("averagePrice")
            # Log a small subset of identifying fields if present.
            extra = {
                k: pos.get(k)
                for k in (
                    "positionId",
                    "taxLotId",
                    "currentDayCost",
                    "currentDayProfitLoss",
                    "maintenanceRequirement",
                    "marketValue",
                )
                if k in pos
            }
            logger.info(
                "[IWM-RAW] acct=%s occ=%s long=%s short=%s avgPrice=%s extra=%s",
                account_id,
                occ_raw,
                long_q,
                short_q,
                avg_p,
                extra or None,
            )
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

    def _build_condor_position(
        underlying: str,
        expiry: str,
        account_id: str,
        lp_pos: dict,
        sp_pos: dict,
        lc_pos: dict,
        sc_pos: dict,
        errors: list[str],
    ) -> Optional[dict]:
        """Shared helper to construct an IRON_CONDOR position from four legs."""
        lp_qty = int(lp_pos.get("longQuantity")  or 0)
        sp_qty = int(sp_pos.get("shortQuantity") or 0)
        lc_qty = int(lc_pos.get("longQuantity")  or 0)
        sc_qty = int(sc_pos.get("shortQuantity") or 0)

        # Trap 4 — assert all leg quantities are identical and non-zero
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
            return None

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
            return None

        # Fallback validation: enforce canonical iron-condor strike ordering
        if not (lp_strike < sp_strike and sp_strike < sc_strike and sc_strike < lc_strike):
            msg = (
                f"reconciler: {underlying} {expiry} — "
                f"invalid condor strike ordering lp={lp_strike} sp={sp_strike} "
                f"sc={sc_strike} lc={lc_strike}; skipping"
            )
            logger.info(msg)
            return None

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

        # Per-leg JSON for positions.legs (same shape as strategy_engine/positions.py).
        # Enables GET /positions to expose avg_price / market_value for dashboard leg prices.
        def _condor_leg_dict(pos: dict, strike: float, right: str, qty_signed: int) -> dict:
            instr = pos.get("instrument") or {}
            mv = pos.get("marketValue")
            sign = 1 if qty_signed > 0 else -1
            return {
                "schwab_symbol": instr.get("symbol"),
                "expiry": expiry,
                "right": right,
                "strike": strike,
                "qty_signed": qty_signed,
                "avg_price": _leg_avg_price(pos, sign),
                "market_value": float(mv) if isinstance(mv, (int, float)) else None,
            }

        # Key order matches strategy_engine._group_legs_into_condor()["legs"].
        legs_struct = {
            "short_put": _condor_leg_dict(sp_pos, float(sp_strike), "P", -quantity),
            "long_put": _condor_leg_dict(lp_pos, float(lp_strike), "P", quantity),
            "short_call": _condor_leg_dict(sc_pos, float(sc_strike), "C", -quantity),
            "long_call": _condor_leg_dict(lc_pos, float(lc_strike), "C", quantity),
        }

        return {
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
            "legs":              4,  # leg count for health-check (in-memory only)
            "legs_struct":       legs_struct,  # persisted to positions.legs JSONB
        }

    for (underlying, expiry), legs in groups.items():
        # Schwab can return multiple position rows for the same option contract
        # (e.g., tax lots / lot-splitting). That can inflate leg counts and cause
        # valid 4-leg structures to fall through into UNKNOWN.
        #
        # Safe normalization: merge rows by OCC contract symbol, summing quantities.
        # This is conservative because the OCC symbol uniquely identifies a contract.
        merged_by_occ: dict[str, dict] = {}
        for pos in legs:
            instr = pos.get("instrument") or {}
            occ_sym = instr.get("symbol")
            if not occ_sym:
                continue
            if occ_sym not in merged_by_occ:
                merged_by_occ[occ_sym] = pos
                continue
            existing = merged_by_occ[occ_sym]
            # Sum quantities (Schwab represents these as floats).
            try:
                existing["longQuantity"] = float(existing.get("longQuantity") or 0.0) + float(pos.get("longQuantity") or 0.0)
                existing["shortQuantity"] = float(existing.get("shortQuantity") or 0.0) + float(pos.get("shortQuantity") or 0.0)
            except Exception:
                pass
            # Preserve the first non-null avg price fields (good enough for fill_credit).
            for k in ("averageLongPrice", "taxLotAverageLongPrice", "averageShortPrice", "taxLotAverageShortPrice", "averagePrice"):
                if existing.get(k) in (None, 0, 0.0) and pos.get(k) not in (None, 0, 0.0):
                    existing[k] = pos.get(k)

        if merged_by_occ:
            legs = list(merged_by_occ.values())

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

        # ── Primary iron condor path (broker-style 4-leg group) ───────────────
        if len(long_puts) == len(short_puts) == len(long_calls) == len(short_calls) == 1:
            lp_pos = long_puts[0]
            sp_pos = short_puts[0]
            lc_pos = long_calls[0]
            sc_pos = short_calls[0]

            condor = _build_condor_position(
                underlying, expiry, account_id,
                lp_pos, sp_pos, lc_pos, sc_pos,
                errors,
            )
            if condor:
                condors.append(condor)
            # Regardless of success, this 4-leg broker-style group is fully
            # handled by the primary path (either as a condor or as an error).
            # Do NOT run the heuristic/non-condor path on these same legs — that
            # would double-count errors or mis-classify broken condors.
            continue

        # ── Non-condor option path + heuristic fallback reconstruction ────────
        # Start from full leg sets, but allow the heuristic below to peel off
        # additional 4-leg iron condors from multi-leg groups. Any leftover legs
        # after that become non-condor strategies.
        remaining_long_puts   = long_puts[:]
        remaining_short_puts  = short_puts[:]
        remaining_long_calls  = long_calls[:]
        remaining_short_calls = short_calls[:]

        def _qty_long(pos: dict) -> int:
            try:
                return int(float(pos.get("longQuantity") or 0))
            except Exception:
                return 0

        def _qty_short(pos: dict) -> int:
            try:
                return int(float(pos.get("shortQuantity") or 0))
            except Exception:
                return 0

        def _clone_leg_with_qty(pos: dict, *, long_qty: int = 0, short_qty: int = 0) -> dict:
            """
            Create a shallow clone of a Schwab leg dict with adjusted quantities.
            Used for safe partitioning of multi-lot/multi-structure pools.
            """
            c = dict(pos)
            c["longQuantity"] = float(long_qty)
            c["shortQuantity"] = float(short_qty)
            return c

        def _decrement_leg_qty(pos: dict, *, long_delta: int = 0, short_delta: int = 0) -> None:
            """In-place decrement of quantities on an existing leg dict."""
            if long_delta:
                pos["longQuantity"] = float(_qty_long(pos) - long_delta)
            if short_delta:
                pos["shortQuantity"] = float(_qty_short(pos) - short_delta)

        # Targeted multi-condor decomposition (shared put spread + multiple call spreads):
        # Observed live example (IWM):
        #   LP 220P qty4, SP 225P qty4,
        #   SC 269C qty2 + LC 274C qty2,
        #   SC 270C qty2 + LC 275C qty2
        #
        # This is not a single condor; it's two condors sharing the same put spread.
        # Only decompose when partitioning is clean and quantities match exactly.
        if (
            len(remaining_long_puts) == 1
            and len(remaining_short_puts) == 1
            and len(remaining_short_calls) >= 2
            and len(remaining_long_calls) >= 2
        ):
            lp_base = remaining_long_puts[0]
            sp_base = remaining_short_puts[0]
            put_qty = _qty_long(lp_base)
            if put_qty > 0 and put_qty == _qty_short(sp_base):
                def _strike_val(p: dict) -> Optional[float]:
                    occ = p.get("_occ") or {}
                    if "strike" in occ:
                        return float(occ["strike"])
                    v = (p.get("instrument") or {}).get("strikePrice")
                    return float(v) if v is not None else None

                lp_strike0 = _strike_val(lp_base)
                sp_strike0 = _strike_val(sp_base)
                shorts_sorted = sorted(
                    remaining_short_calls, key=lambda p: (_strike_val(p) or 0.0)
                )
                longs_sorted = sorted(
                    remaining_long_calls, key=lambda p: (_strike_val(p) or 0.0)
                )

                # Pair shorts/longs by rank (smallest-short with smallest-long, etc.)
                pairs: list[tuple[dict, dict, int]] = []
                if lp_strike0 is not None and sp_strike0 is not None:
                    for sc_pos, lc_pos in zip(shorts_sorted, longs_sorted, strict=False):
                        sc_qty = _qty_short(sc_pos)
                        lc_qty = _qty_long(lc_pos)
                        sc_k = _strike_val(sc_pos)
                        lc_k = _strike_val(lc_pos)
                        if (
                            sc_qty <= 0
                            or lc_qty <= 0
                            or sc_qty != lc_qty
                            or sc_k is None
                            or lc_k is None
                        ):
                            pairs = []
                            break
                        # Condor strike ordering must hold for each partition
                        if not (lp_strike0 < sp_strike0 < sc_k < lc_k):
                            pairs = []
                            break
                        pairs.append((sc_pos, lc_pos, sc_qty))

                if pairs and sum(q for _, _, q in pairs) == put_qty and len(pairs) >= 2:
                    # Extract one condor per call-spread pair
                    _condors_before = len(condors)
                    for sc_pos, lc_pos, q in pairs:
                        lp_sub = _clone_leg_with_qty(lp_base, long_qty=q, short_qty=0)
                        sp_sub = _clone_leg_with_qty(sp_base, long_qty=0, short_qty=q)
                        sc_sub = _clone_leg_with_qty(sc_pos, long_qty=0, short_qty=q)
                        lc_sub = _clone_leg_with_qty(lc_pos, long_qty=q, short_qty=0)

                        condor = _build_condor_position(
                            underlying,
                            expiry,
                            account_id,
                            lp_sub,
                            sp_sub,
                            lc_sub,
                            sc_sub,
                            errors,
                        )
                        if condor is None:
                            # If any extracted condor is invalid, abort and fall back to UNKNOWN.
                            del condors[_condors_before:]
                            pairs = []
                            break
                        condors.append(condor)

                        # Consume quantities from the working pool
                        _decrement_leg_qty(lp_base, long_delta=q)
                        _decrement_leg_qty(sp_base, short_delta=q)
                        _decrement_leg_qty(sc_pos, short_delta=q)
                        _decrement_leg_qty(lc_pos, long_delta=q)

                    # Remove fully-consumed legs from remaining lists
                    remaining_short_calls = [p for p in remaining_short_calls if _qty_short(p) > 0]
                    remaining_long_calls  = [p for p in remaining_long_calls if _qty_long(p) > 0]
                    remaining_long_puts   = [p for p in remaining_long_puts if _qty_long(p) > 0]
                    remaining_short_puts  = [p for p in remaining_short_puts if _qty_short(p) > 0]

        # Heuristic iron-condor reconstruction for unassigned legs:
        #   - exactly 1 LP, 1 SP, 1 SC, 1 LC
        #   - identical quantity across all 4 legs
        #   - strike ordering lp < sp < sc < lc
        while (
            remaining_long_puts
            and remaining_short_puts
            and remaining_long_calls
            and remaining_short_calls
        ):
            found = False
            for lp_pos in list(remaining_long_puts):
                for sp_pos in list(remaining_short_puts):
                    for sc_pos in list(remaining_short_calls):
                        for lc_pos in list(remaining_long_calls):
                            condor = _build_condor_position(
                                underlying, expiry, account_id,
                                lp_pos, sp_pos, lc_pos, sc_pos,
                                errors,
                            )
                            if condor is None:
                                continue
                            # Valid condor — record and remove these legs
                            condors.append(condor)
                            remaining_long_puts.remove(lp_pos)
                            remaining_short_puts.remove(sp_pos)
                            remaining_short_calls.remove(sc_pos)
                            remaining_long_calls.remove(lc_pos)
                            found = True
                            break
                        if found:
                            break
                    if found:
                        break
                if found:
                    break
            if not found:
                break  # no further 4-leg structures can be formed safely

        # Whatever legs remain become non-condor option strategies
        all_legs = remaining_long_puts + remaining_short_puts + remaining_long_calls + remaining_short_calls
        n_legs   = len(all_legs)

        if n_legs == 0:
            # All legs in this (symbol, expiry) bucket were consumed into condors.
            continue

        def _occ_strike(pos: dict) -> Optional[float]:
            occ = pos.get("_occ")
            if occ and "strike" in occ:
                return occ["strike"]
            v = (pos.get("instrument") or {}).get("strikePrice")
            return float(v) if v is not None else None

        # Build leg details for legs_json (enriched for auditability)
        leg_details = []
        for pos in all_legs:
            instr = pos.get("instrument") or {}
            occ   = pos.get("_occ") or {}
            long_qty  = int(pos.get("longQuantity")  or 0)
            short_qty = int(pos.get("shortQuantity") or 0)
            side      = "long" if long_qty > 0 else ("short" if short_qty > 0 else "flat")
            quantity  = abs(long_qty or short_qty)
            leg_details.append({
                "symbol":      underlying,
                "expiry":      expiry,
                "occ_symbol":  instr.get("symbol"),
                "option_type": occ.get("option_type"),
                "strike":      occ.get("strike"),
                "long_qty":    long_qty,
                "short_qty":   short_qty,
                "side":        side,
                "quantity":    quantity,
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
            has_long  = len(remaining_long_puts) + len(remaining_long_calls) == 1
            has_short = len(remaining_short_puts) + len(remaining_short_calls) == 1
            if has_long and has_short:
                # Same type → vertical spread; different types → strangle/straddle
                same_type = (
                    (len(remaining_long_puts) == 1 and len(remaining_short_puts) == 1) or
                    (len(remaining_long_calls) == 1 and len(remaining_short_calls) == 1)
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
        sc_strike = _occ_strike(remaining_short_calls[0]) if remaining_short_calls else None
        sp_strike = _occ_strike(remaining_short_puts[0])  if remaining_short_puts  else None
        lc_strike = _occ_strike(remaining_long_calls[0])  if remaining_long_calls  else None
        lp_strike = _occ_strike(remaining_long_puts[0])   if remaining_long_puts   else None

        # Position key for non-condors — includes strategy to avoid collision.
        # Suffix :{account_id} is appended to match the format produced by the
        # migrate_orders_schema one-time migration (idempotent: migration guard
        # checks NOT LIKE '%:' || account_id, so rows with this suffix won't be
        # double-suffixed on subsequent migrate runs).
        strike_parts = [
            str(s) for s in (lp_strike, sp_strike, sc_strike, lc_strike)
            if s is not None
        ]
        position_key = (
            f"{underlying}_{expiry}_{strategy}_{account_id}_"
            + ("-".join(strike_parts) if strike_parts else "nostrike")
            + f":{account_id}"
        )

        # Structural imbalance detection:
        # - Single-leg options and recognised 2-leg structures are considered safe.
        # - Multi-leg options with strategy UNKNOWN are quarantined as 'imbalanced'.
        status = "open"
        if n_legs > 1 and strategy == "UNKNOWN":
            status = "imbalanced"
            logger.warning(
                "[RECONCILER-IMBALANCE] %s %s - %s",
                underlying,
                expiry,
                f"{n_legs} legs, strategy={strategy}, legs_json={leg_details}",
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
            "status":            status,
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
    sw_acct  = schwab_pos.get("account_id")

    # EQUITY: match by symbol + strategy + account_id (prevents cross-account collision
    # when the same equity symbol is held in multiple accounts).
    if strategy == "EQUITY":
        for db in db_positions:
            if db.get("symbol") != sym or db.get("strategy") != "EQUITY":
                continue
            # Enforce account match when both sides have account_id populated
            if sw_acct and db.get("account_id") and db.get("account_id") != sw_acct:
                continue
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

        # For non-condor positions, also enforce account_id to prevent cross-account
        # false matches (e.g. same option symbol/strike in both 5760 and 8096).
        if strategy != "IRON_CONDOR" and sw_acct and db.get("account_id") and db.get("account_id") != sw_acct:
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

    # ── Minimal stale-order cleanup (48h rule) ────────────────────────────────
    try:
        with engine.begin() as _oconn:
            # Paper + live_dry_run orders can be safely auto-cancelled when stale.
            stale_rows = _oconn.execute(text("""
                UPDATE orders
                SET status = 'cancelled'
                WHERE status NOT IN ('filled', 'rejected', 'cancelled')
                  AND source IN ('paper', 'live_dry_run')
                  AND created_at < (NOW() - INTERVAL '48 hours')
                RETURNING id, symbol, source
            """)).fetchall()
            for r in stale_rows:
                logger.info(
                    "[ORDER-CLEANUP] Auto-cancelled stale %s order %s for %s",
                    r.source,
                    r.id,
                    r.symbol,
                )

            # For true live orders, do NOT rewrite status locally — just log that
            # they are stale so operators can investigate, and rely on the 24h
            # correlation window to exclude them from partial-fill detection.
            stale_live = _oconn.execute(text("""
                SELECT id, symbol
                FROM orders
                WHERE status NOT IN ('filled', 'rejected', 'cancelled')
                  AND source = 'live'
                  AND created_at < (NOW() - INTERVAL '48 hours')
            """)).fetchall()
            for r in stale_live:
                logger.info(
                    "[ORDER-CLEANUP] Live order %s for %s is older than 48h; "
                    "excluded from partial-fill correlation but status not changed",
                    r.id,
                    r.symbol,
                )
    except Exception as e:
        logger.warning(f"reconcile: stale order cleanup failed — {e}")

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

    # Build an index of active routed orders for partial-fill detection.
    with engine.connect() as _ord_conn:
        active_order_index = _index_active_orders_for_partials(_ord_conn)

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

    # Order-aware partial-fill detection: for small non-condor option positions
    # (1–2 legs) that structurally look like valid trades, check if their legs
    # are only a subset of an active routed multi-leg order. If so, quarantine
    # as 'imbalanced' so automation is blocked while preserving full leg detail.
    for sp in all_schwab_positions:
        if sp.get("strategy") in ("EQUITY",) or sp.get("status") == "imbalanced":
            continue
        legs = int(sp.get("legs") or 0)
        if legs not in (1, 2):
            continue
        acct = str(sp.get("account_id") or "")
        sym  = str(sp.get("symbol") or "")
        exp  = str(sp.get("expiry") or "")
        key  = (acct, sym, exp)
        orders = active_order_index.get(key, [])
        if not orders:
            continue

        legs_raw = sp.get("legs_json")
        try:
            legs_list = (
                legs_raw if isinstance(legs_raw, list)
                else json.loads(legs_raw or "[]")
            )
        except Exception:
            continue
        occs_pos = {
            str(l.get("occ_symbol"))
            for l in legs_list
            if l.get("occ_symbol")
        }
        if not occs_pos:
            continue

        for order in orders:
            occs_order = order.get("occs") or set()
            if not occs_order:
                continue
            if not occs_pos.issubset(occs_order):
                continue

            qty_order = int(order.get("quantity") or 0)
            qty_pos   = int(sp.get("quantity") or 0)
            full_match = (occs_pos == occs_order) and (
                qty_order == 0 or qty_pos == qty_order
            )
            if full_match:
                # Clean realization of the routed order — treat as normal.
                continue

            # Partial realization of a routed multi-leg order — quarantine.
            sp["status"] = "imbalanced"
            logger.warning(
                "[PARTIAL-FILL-DETECTED] %s %s - matched active order %s, broker legs are incomplete",
                sym, exp, order["id"],
            )
            break

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
                   strategy,
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
                        quantity, fill_credit, legs_json, legs,
                        opened_at, status, source, position_key, dte
                    ) VALUES (
                        :account_id, :symbol, :expiry, :strategy,
                        :long_put_strike, :short_put_strike,
                        :short_call_strike, :long_call_strike,
                        :quantity, :fill_credit, :legs_json, CAST(:legs AS JSONB),
                        :opened_at, :status, 'manual', :position_key, :dte
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
                    "legs": (
                        json.dumps(sp["legs_struct"])
                        if sp.get("legs_struct") is not None
                        else None
                    ),
                    "opened_at":         now,
                    "status":            sp.get("status", "open"),
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

                # Persist quarantine state changes (open → imbalanced) on updates.
                # Without this, logs can show [RECONCILER-IMBALANCE] but DB stays stale.
                sp_status = sp.get("status")
                if sp_status and db_match.get("status") != sp_status:
                    changes["status"] = (db_match.get("status"), sp_status)

                # Refresh legs_json when provided (especially important for imbalanced rows).
                if sp.get("legs_json") is not None and db_match.get("legs_json") != sp.get("legs_json"):
                    changes["legs_json"] = (db_match.get("legs_json"), sp.get("legs_json"))

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
                            status               = COALESCE(:status, status),
                            legs_json            = COALESCE(:legs_json, legs_json),
                            legs                 = COALESCE(CAST(:legs AS JSONB), legs),
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
                        "status":            sp.get("status"),
                        "legs_json":         sp.get("legs_json"),
                        "legs": (
                            json.dumps(sp["legs_struct"])
                            if sp.get("legs_struct") is not None
                            else None
                        ),
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
                    # No discrepancy — reset strike counter, update timestamps;
                    # refresh condor per-leg JSON from Schwab when present.
                    conn.execute(text("""
                        UPDATE positions
                        SET last_reconciled_at  = :now,
                            closure_strikes     = 0,
                            last_seen_in_schwab = :now,
                            legs                  = COALESCE(CAST(:legs AS JSONB), legs)
                        WHERE id = :id
                    """), {
                        "now": now,
                        "id": db_match["id"],
                        "legs": (
                            json.dumps(sp["legs_struct"])
                            if sp.get("legs_struct") is not None
                            else None
                        ),
                    })

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
    
    try:
        migrate_orders_schema(engine)

        try:
            client = get_schwab_client()
        except Exception as e:
            logger.error(f"run_scheduled_reconciliation: could not init Schwab client - {e}")
            return  # The finally block will still run before this return executes!

        ts = datetime.now(timezone.utc).isoformat()

        pos_summary = reconcile(engine, client)
        nav_summary = reconcile_nav(engine, client)

        full_summary = {
            "ts": ts,
            "positions": pos_summary,
            "nav": nav_summary,
        }

        logger.info(f"Reconciliation summary: {json.dumps(full_summary, default=str)}")

        # Append to reconciler.log
        try:
            with open(RECONCILER_LOG, "a") as f:
                f.write(json.dumps(full_summary, default=str) + "\n")
        except Exception as e:
            logger.error(f"Failed to append to reconciler log: {e}")

        # Record per-account run timestamps in reconciler_runs table.
        run_at = datetime.now(timezone.utc)
        nav_accounts = nav_summary.get("accounts") or {}
        if nav_accounts:
            try:
                with engine.begin() as conn:
                    for acct_id, nav_val in nav_accounts.items():
                        conn.execute(text("""
                            INSERT INTO reconciler_runs (account_id, run_at, nav, created_at)
                            VALUES (:account_id, :run_at, :nav, :created_at)
                        """), {
                            "account_id": str(acct_id),
                            "run_at":     run_at,
                            "nav":        float(nav_val) if nav_val is not None else None,
                            "created_at": run_at,
                        })
                logger.info(
                    "run_scheduled_reconciliation: recorded reconciler_runs for %d account(s)",
                    len(nav_accounts),
                )
            except Exception as e:
                logger.warning("run_scheduled_reconciliation: reconciler_runs insert failed — %s", e)

    finally:
        # THIS IS THE CURE
        engine.dispose()
        logger.info("Database engine disposed to prevent connection leaks.")
   


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
