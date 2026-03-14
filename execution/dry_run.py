"""
execution/dry_run.py — Paper trading simulation

Simulates order fills at mid-price for TRADING_MODE=paper.

simulate_fill():
  - Receives the order_id of the already-inserted pending orders row
  - Does NOT insert a new orders row — UPDATEs the existing one
  - Looks up mid-prices for all four legs from option_quotes
  - Writes a paper position to the positions table (account_id='PAPER')
  - Returns order_id

Never called when TRADING_MODE=live.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Helpers ───────────────────────────────────────────────────────────────────
def _get_latest_snapshot_id(conn) -> Optional[int]:
    row = conn.execute(text("""
        SELECT id FROM snapshot_runs
        WHERE status IN ('ok', 'partial')
        ORDER BY ts DESC
        LIMIT 1
    """)).fetchone()
    return row.id if row else None


def _get_mid(
    conn,
    symbol:       str,
    snapshot_id:  int,
    expiry:       str,
    strike:       float,
    option_right: str,          # 'P' or 'C'
) -> Optional[float]:
    """
    Returns mid = (bid + ask) / 2 for a single option leg from option_quotes.
    Returns None if the row is missing or bid/ask are NULL.
    """
    row = conn.execute(text("""
        SELECT bid, ask
        FROM option_quotes
        WHERE symbol       = :symbol
          AND snapshot_id  = :snapshot_id
          AND expiry       = :expiry
          AND strike       = :strike
          AND option_right = :option_right
        LIMIT 1
    """), {
        "symbol":       symbol,
        "snapshot_id":  snapshot_id,
        "expiry":       expiry,
        "strike":       strike,
        "option_right": option_right,
    }).fetchone()

    if not row or row.bid is None or row.ask is None:
        return None
    return (float(row.bid) + float(row.ask)) / 2.0


# ── Main entry point ──────────────────────────────────────────────────────────
def simulate_fill(candidate_json: dict, quantity: int, order_id: int) -> int:
    """
    Simulates a paper fill for an iron condor.

    candidate_json is the raw dict from the trade_candidates row.
    order_id is the id of the pending row already inserted by executor.py.

    Steps:
      1. Look up mid-prices for all four legs from the latest snapshot
      2. net_fill = short_put_mid + short_call_mid - long_put_mid - long_call_mid
         Falls back to stored net_credit if any mid is unavailable
      3. UPDATE orders SET status='filled', fill_price=net_fill, filled_at=now()
      4. INSERT into positions with account_id='PAPER'
      5. Return order_id
    """
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        symbol     = candidate_json["symbol"]
        expiry     = candidate_json["expiry"]
        net_credit = float(candidate_json["net_credit"])

        # 1. Fetch mid-prices from latest snapshot
        snapshot_id    = _get_latest_snapshot_id(conn)
        short_put_mid  = None
        short_call_mid = None
        long_put_mid   = None
        long_call_mid  = None

        if snapshot_id is not None:
            short_put_mid  = _get_mid(conn, symbol, snapshot_id, expiry, candidate_json["short_put_strike"],  "P")
            short_call_mid = _get_mid(conn, symbol, snapshot_id, expiry, candidate_json["short_call_strike"], "C")
            long_put_mid   = _get_mid(conn, symbol, snapshot_id, expiry, candidate_json["long_put_strike"],   "P")
            long_call_mid  = _get_mid(conn, symbol, snapshot_id, expiry, candidate_json["long_call_strike"],  "C")

        # 2. Compute net_fill
        if all(x is not None for x in [short_put_mid, short_call_mid, long_put_mid, long_call_mid]):
            net_fill = round(
                short_put_mid + short_call_mid - long_put_mid - long_call_mid,
                4,
            )
            logger.info(
                f"simulate_fill: order_id={order_id} symbol={symbol} "
                f"net_fill={net_fill:.4f} (live mids from snapshot_id={snapshot_id})"
            )
        else:
            net_fill = net_credit
            logger.warning(
                f"simulate_fill: order_id={order_id} {symbol} — "
                f"could not fetch all leg mids (snapshot_id={snapshot_id}), "
                f"falling back to stored net_credit={net_credit}"
            )

        # 3. Update orders row — status='filled'
        conn.execute(text("""
            UPDATE orders
            SET status     = 'filled',
                fill_price = :fill_price,
                filled_at  = :filled_at
            WHERE id = :order_id
        """), {
            "fill_price": net_fill,
            "filled_at":  datetime.now(timezone.utc),
            "order_id":   order_id,
        })

        # 4. Insert position row
        conn.execute(text("""
            INSERT INTO positions (
                account_id,
                symbol,
                expiry,
                strategy,
                long_put_strike,
                short_put_strike,
                short_call_strike,
                long_call_strike,
                quantity,
                fill_credit,
                opened_at,
                status,
                order_id
            ) VALUES (
                'PAPER',
                :symbol,
                :expiry,
                'IRON_CONDOR',
                :long_put_strike,
                :short_put_strike,
                :short_call_strike,
                :long_call_strike,
                :quantity,
                :fill_credit,
                :opened_at,
                'open',
                :order_id
            )
        """), {
            "symbol":            symbol,
            "expiry":            expiry,
            "long_put_strike":   candidate_json["long_put_strike"],
            "short_put_strike":  candidate_json["short_put_strike"],
            "short_call_strike": candidate_json["short_call_strike"],
            "long_call_strike":  candidate_json["long_call_strike"],
            "quantity":          quantity,
            "fill_credit":       net_fill,
            "opened_at":         datetime.now(timezone.utc),
            "order_id":          order_id,
        })

        logger.info(
            f"simulate_fill complete: order_id={order_id} symbol={symbol} "
            f"expiry={expiry} qty={quantity} fill_credit={net_fill:.4f} account=PAPER"
        )

    return order_id
