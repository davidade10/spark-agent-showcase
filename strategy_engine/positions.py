"""
strategy_engine/positions.py

Pull open positions from Schwab and sync to the local TimescaleDB `positions` table.
- Detect iron condors by grouping 4 option legs with same underlying + expiry.
- Store a single row per condor with legs JSON, entry credit, max risk, DTE, and last mark.
- This file becomes the source of truth for portfolio state + exit monitoring.

IMPORTANT:
- Read-only: this module does NOT place trades.
- Never print token.json contents. Never log access_token/refresh_token/id_token.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, text
from loguru import logger

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from data_layer.provider import get_schwab_client

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Matches Schwab option position symbols like: "NVDA  260417P00150000"
OCC_RE = re.compile(r"^([A-Z0-9.]{1,12})\s+(\d{6})([CP])(\d{8})$")


@dataclass
class Leg:
    underlying: str
    expiry: date
    right: str              # 'C' or 'P'
    strike: float
    qty_signed: int         # + long, - short
    avg_price: Optional[float]
    market_value: Optional[float]
    schwab_symbol: str


def _parse_option_symbol(s: str) -> Optional[Tuple[str, date, str, float]]:
    s = s.strip()
    m = OCC_RE.match(s)
    if not m:
        return None
    underlying, yymmdd, right, strike8 = m.groups()
    expiry = datetime.strptime(yymmdd, "%y%m%d").date()
    strike = int(strike8) / 1000.0
    return underlying, expiry, right, strike


def _pick_avg_price(pos: Dict[str, Any], qty_signed: int) -> Optional[float]:
    if qty_signed < 0:
        for k in ("averageShortPrice", "taxLotAverageShortPrice", "averagePrice"):
            v = pos.get(k)
            if isinstance(v, (int, float)) and float(v) > 0:
                return float(v)
    else:
        for k in ("averageLongPrice", "taxLotAverageLongPrice", "averagePrice"):
            v = pos.get(k)
            if isinstance(v, (int, float)) and float(v) > 0:
                return float(v)
    return None


def _qty_signed(pos: Dict[str, Any]) -> int:
    long_q = pos.get("longQuantity") or 0
    short_q = pos.get("shortQuantity") or 0
    try:
        return int(round(float(long_q) - float(short_q)))
    except Exception:
        return 0


def _leg_from_position(pos: Dict[str, Any]) -> Optional[Leg]:
    instr = pos.get("instrument") or {}
    raw_symbol = instr.get("symbol") or pos.get("symbol")
    if not raw_symbol:
        return None

    parsed = _parse_option_symbol(raw_symbol)
    if not parsed:
        return None

    underlying, expiry, right, strike = parsed
    qty = _qty_signed(pos)
    if qty == 0:
        return None

    return Leg(
        underlying=underlying,
        expiry=expiry,
        right=right,
        strike=strike,
        qty_signed=qty,
        avg_price=_pick_avg_price(pos, qty),
        market_value=(float(pos["marketValue"]) if isinstance(pos.get("marketValue"), (int, float)) else None),
        schwab_symbol=raw_symbol,
    )


def _group_legs_into_condor(legs: List[Leg]) -> Optional[Dict[str, Any]]:
    puts = [l for l in legs if l.right == "P"]
    calls = [l for l in legs if l.right == "C"]
    if len(puts) < 2 or len(calls) < 2:
        return None

    short_puts = sorted([l for l in puts if l.qty_signed < 0], key=lambda x: x.strike, reverse=True)
    long_puts  = sorted([l for l in puts if l.qty_signed > 0], key=lambda x: x.strike)

    short_calls = sorted([l for l in calls if l.qty_signed < 0], key=lambda x: x.strike)
    long_calls  = sorted([l for l in calls if l.qty_signed > 0], key=lambda x: x.strike, reverse=True)

    if not short_puts or not long_puts or not short_calls or not long_calls:
        return None

    sp, lp, sc, lc = short_puts[0], long_puts[0], short_calls[0], long_calls[0]

    # sanity: condor structure
    if not (lp.strike < sp.strike and sc.strike < lc.strike):
        return None
    if not (sp.expiry == lp.expiry == sc.expiry == lc.expiry):
        return None

    qty = min(abs(sp.qty_signed), abs(sc.qty_signed), lp.qty_signed, lc.qty_signed)
    if qty <= 0:
        return None

    def _as_dict(l: Leg) -> Dict[str, Any]:
        return {
            "schwab_symbol": l.schwab_symbol,
            "expiry": l.expiry.isoformat(),
            "right": l.right,
            "strike": l.strike,
            "qty_signed": l.qty_signed,
            "avg_price": l.avg_price,
            "market_value": l.market_value,
        }

    width_put = sp.strike - lp.strike
    width_call = lc.strike - sc.strike
    max_width = max(width_put, width_call)

    # Entry credit estimate (best-effort). If any avg_price missing, leave None.
    entry_credit = None
    if all(x.avg_price is not None for x in (sp, lp, sc, lc)):
        credit_per_contract = (sp.avg_price + sc.avg_price) - (lp.avg_price + lc.avg_price)
        entry_credit = round(credit_per_contract * 100.0 * qty, 2)

    # Current debit-to-close estimate using net market value
    mv_vals = [x.market_value for x in (sp, lp, sc, lc) if x.market_value is not None]
    net_mv = round(sum(mv_vals), 2) if mv_vals else None
    debit_to_close = abs(net_mv) if net_mv is not None else None

    max_risk = None
    if entry_credit is not None:
        max_risk = round(max_width * 100.0 * qty - entry_credit, 2)

    position_key = f"{sp.underlying}:{sp.expiry.isoformat()}:{lp.strike}-{sp.strike}:{sc.strike}-{lc.strike}:{qty}"

    return {
        "symbol": sp.underlying,
        "expiry": sp.expiry,
        "qty": qty,
        "position_key": position_key,
        "legs": {
            "short_put": _as_dict(sp),
            "long_put": _as_dict(lp),
            "short_call": _as_dict(sc),
            "long_call": _as_dict(lc),
        },
        "entry_credit": entry_credit,
        "max_risk": max_risk,
        "net_mv": net_mv,
        "debit_to_close": debit_to_close,
        "dte": (sp.expiry - date.today()).days,
    }


def sync_positions() -> None:
    """
    Main entrypoint:
    - Pull positions from Schwab across accounts.
    - Detect iron condors.
    - Upsert into positions table.
    - Mark missing ones as closed (best-effort).
    """
    client = get_schwab_client()
    engine = create_engine(DB_URL)

    resp = client.get_accounts(fields=client.Account.Fields.POSITIONS)
    resp.raise_for_status()
    accounts = resp.json()

    now = datetime.now(timezone.utc)

    seen_keys: set[Tuple[str, str]] = set()  # (account_id, position_key)
    total_upserted = 0

    with engine.begin() as conn:
        for acc in accounts:
            sa = acc.get("securitiesAccount", {})
            account_id = str(sa.get("accountNumber", "unknown"))

            pos_list = sa.get("positions") or []
            option_positions = [p for p in pos_list if (p.get("instrument") or {}).get("assetType") == "OPTION"]

            legs: List[Leg] = []
            for p in option_positions:
                leg = _leg_from_position(p)
                if leg:
                    legs.append(leg)

            # group by (underlying, expiry)
            groups: Dict[Tuple[str, date], List[Leg]] = {}
            for l in legs:
                groups.setdefault((l.underlying, l.expiry), []).append(l)

            for (underlying, expiry), glegs in groups.items():
                condor = _group_legs_into_condor(glegs)
                if not condor:
                    continue

                key = condor["position_key"]
                seen_keys.add((account_id, key))

                entry_credit = condor["entry_credit"]
                debit_to_close = condor["debit_to_close"]
                pnl = None
                if entry_credit is not None and debit_to_close is not None:
                    pnl = round(entry_credit - debit_to_close, 2)

                meta = {
                    "qty": condor["qty"],
                    "net_mv": condor["net_mv"],
                    "debit_to_close": debit_to_close,
                    "unrealized_pnl_est": pnl,
                }

                conn.execute(
                    text("""
                        INSERT INTO positions
                          (position_key, account_id, symbol, strategy, qty, legs,
                           entry_credit, max_risk, net_delta, expiry, dte,
                           status, opened_at, last_reconciled_at, meta)
                        VALUES
                          (:position_key, :account_id, :symbol, 'iron_condor', :qty, CAST(:legs AS JSONB),
                           :entry_credit, :max_risk, NULL, :expiry, :dte,
                           'open', COALESCE(:opened_at, NOW()), :reconciled_at, CAST(:meta AS JSONB))
                        ON CONFLICT (position_key) DO UPDATE SET
                          account_id = EXCLUDED.account_id,
                          symbol = EXCLUDED.symbol,
                          qty = EXCLUDED.qty,
                          legs = EXCLUDED.legs,
                          entry_credit = COALESCE(positions.entry_credit, EXCLUDED.entry_credit),
                          max_risk = COALESCE(positions.max_risk, EXCLUDED.max_risk),
                          expiry = EXCLUDED.expiry,
                          dte = EXCLUDED.dte,
                          status = 'open',
                          last_reconciled_at = EXCLUDED.last_reconciled_at,
                          meta = EXCLUDED.meta
                    """),
                    {
                        "position_key": key,
                        "account_id": account_id,
                        "symbol": underlying,
                        "qty": int(condor["qty"]),
                        "legs": __import__("json").dumps(condor["legs"]),
                        "entry_credit": entry_credit,
                        "max_risk": condor["max_risk"],
                        "expiry": expiry,
                        "dte": int(condor["dte"]),
                        "opened_at": None,
                        "reconciled_at": now,
                        "meta": __import__("json").dumps(meta),
                    },
                )
                total_upserted += 1

        # Mark old open positions as closed if they weren't seen in Schwab pull
        rows = conn.execute(text("""
            SELECT id, account_id, position_key
            FROM positions
            WHERE status = 'open' AND position_key IS NOT NULL
        """)).fetchall()

        closed = 0
        for r in rows:
            acct = str(r[1])
            pkey = str(r[2])
            if (acct, pkey) not in seen_keys:
                conn.execute(text("""
                    UPDATE positions
                    SET status='closed', closed_at=NOW(), last_reconciled_at=NOW()
                    WHERE id=:id
                """), {"id": r[0]})
                closed += 1

    logger.info(f"Positions sync complete. Upserted={total_upserted}, closed={closed}")


if __name__ == "__main__":
    sync_positions()

  