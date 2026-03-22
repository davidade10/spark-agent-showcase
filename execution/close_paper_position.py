"""
execution/close_paper_position.py

Closes a paper position and records the outcome.

All three writes (trade_outcomes INSERT, positions UPDATE, orders INSERT)
execute in a single transaction — fully rolled back on any failure.

Decision-id tracing is best-effort: if the chain
  positions.order_id → orders.candidate_id → trade_decisions.candidate_id
cannot be resolved, trade_outcomes.decision_id is written as NULL with a
logged warning. The close proceeds normally.

Never modifies dry_run.py, reconciler.py, or order_state.py.
Never alters existing table schemas.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def close_paper_position(
    position_id: int,
    exit_debit_per_contract: float,
    exit_reason: str = "MANUAL",
) -> dict:
    """
    Close a paper position and record the outcome.

    Args:
        position_id:             DB id of the positions row to close.
        exit_debit_per_contract: Cost to buy back the spread (per contract,
                                 expressed as per-share price — same unit as
                                 fill_credit).  E.g. 0.30 means $30/contract.
        exit_reason:             Label stored in trade_outcomes.exit_reason
                                 and positions.close_reason.

    Returns:
        {
            "success":          True,
            "position_id":      int,
            "pnl":              float,   # dollars, rounded to 2dp
            "exit_debit":       float,
            "exit_reason":      str,
            "trade_outcome_id": int | None,
        }

    Raises:
        ValueError if:
          - position not found
          - account_id is not 'PAPER'
          - status is not 'open'
    """
    engine = create_engine(DB_URL, pool_pre_ping=True)

    with engine.begin() as conn:

        # ── 1. Load and validate position ─────────────────────────────────────
        pos_row = conn.execute(text("""
            SELECT id, account_id, symbol, status,
                   fill_credit, quantity, order_id
            FROM positions
            WHERE id = :pid
        """), {"pid": position_id}).fetchone()

        if pos_row is None:
            raise ValueError(
                f"close_paper_position: position id={position_id} not found"
            )

        if pos_row.account_id != "PAPER":
            raise ValueError(
                f"close_paper_position: position id={position_id} "
                f"has account_id={pos_row.account_id!r}, expected 'PAPER'"
            )

        if pos_row.status != "open":
            raise ValueError(
                f"close_paper_position: position id={position_id} "
                f"has status={pos_row.status!r}, expected 'open'"
            )

        fill_credit = float(pos_row.fill_credit or 0.0)
        quantity    = int(pos_row.quantity or 1)
        symbol      = pos_row.symbol or "UNKNOWN"
        order_id    = pos_row.order_id

        # ── 2. Best-effort decision_id trace ──────────────────────────────────
        # Chain: positions.order_id → orders.candidate_id
        #        → trade_decisions.candidate_id → trade_decisions.id
        decision_id: Optional[int] = None

        if order_id is not None:
            td_row = conn.execute(text("""
                SELECT td.id
                FROM trade_decisions td
                JOIN orders o ON o.candidate_id = td.candidate_id
                WHERE o.id       = :order_id
                  AND td.decision = 'approved'
                ORDER BY td.id DESC
                LIMIT 1
            """), {"order_id": order_id}).fetchone()

            if td_row is not None:
                decision_id = int(td_row.id)
            else:
                logger.warning(
                    "close_paper_position: could not trace decision_id "
                    "for position_id=%d order_id=%d — writing NULL to trade_outcomes",
                    position_id, order_id,
                )
        else:
            logger.warning(
                "close_paper_position: position_id=%d has no order_id — "
                "decision_id will be NULL in trade_outcomes",
                position_id,
            )

        # ── 3. Compute P&L ────────────────────────────────────────────────────
        # pnl = (credit_collected - cost_to_close) × contracts × 100 shares
        pnl = round((fill_credit - exit_debit_per_contract) * quantity * 100, 2)
        now = datetime.now(timezone.utc)

        # ── 4. Write trade_outcomes ───────────────────────────────────────────
        outcome_row = conn.execute(text("""
            INSERT INTO trade_outcomes (
                decision_id,
                account_id,
                entry_credit,
                exit_debit,
                pnl,
                exit_reason,
                closed_at
            ) VALUES (
                :decision_id,
                'PAPER',
                :entry_credit,
                :exit_debit,
                :pnl,
                :exit_reason,
                :closed_at
            )
            RETURNING id
        """), {
            "decision_id":  decision_id,
            "entry_credit": fill_credit,
            "exit_debit":   exit_debit_per_contract,
            "pnl":          pnl,
            "exit_reason":  exit_reason,
            "closed_at":    now,
        }).fetchone()

        trade_outcome_id = int(outcome_row.id) if outcome_row else None

        # ── 5. Close the position row ─────────────────────────────────────────
        conn.execute(text("""
            UPDATE positions
            SET status       = 'closed',
                close_reason = :close_reason,
                closed_at    = :closed_at
            WHERE id = :pid
        """), {
            "close_reason": exit_reason,
            "closed_at":    now,
            "pid":          position_id,
        })

        # ── 6. Record close order ─────────────────────────────────────────────
        # Mirrors the open order pattern from dry_run.simulate_fill —
        # status='filled', source='paper_close' identifies it as a close.
        conn.execute(text("""
            INSERT INTO orders (
                account_id,
                symbol,
                status,
                source,
                fill_price,
                quantity,
                created_at,
                filled_at
            ) VALUES (
                'PAPER',
                :symbol,
                'filled',
                'paper_close',
                :fill_price,
                :quantity,
                :created_at,
                :filled_at
            )
        """), {
            "symbol":     symbol,
            "fill_price": exit_debit_per_contract,
            "quantity":   quantity,
            "created_at": now,
            "filled_at":  now,
        })

    logger.info(
        "close_paper_position: CLOSED position_id=%d symbol=%s "
        "pnl=$%.2f exit_debit=%.4f reason=%s outcome_id=%s",
        position_id, symbol, pnl,
        exit_debit_per_contract, exit_reason, trade_outcome_id,
    )

    return {
        "success":          True,
        "position_id":      position_id,
        "pnl":              pnl,
        "exit_debit":       exit_debit_per_contract,
        "exit_reason":      exit_reason,
        "trade_outcome_id": trade_outcome_id,
    }


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Close a paper position and record the outcome in trade_outcomes.",
    )
    parser.add_argument(
        "--position-id", type=int, required=True,
        help="DB id of the positions row to close",
    )
    parser.add_argument(
        "--exit-debit", type=float, required=True,
        help="Cost to buy back the spread, per contract per share (e.g. 0.90)",
    )
    parser.add_argument(
        "--exit-reason", type=str, default="MANUAL_CLOSE",
        help="Label stored in trade_outcomes.exit_reason (default: MANUAL_CLOSE)",
    )
    args = parser.parse_args()

    try:
        result = close_paper_position(
            position_id             = args.position_id,
            exit_debit_per_contract = args.exit_debit,
            exit_reason             = args.exit_reason,
        )
        print(f"✓ Closed position_id={result['position_id']}")
        print(f"  P&L:          ${result['pnl']:.2f}")
        print(f"  Exit debit:   ${result['exit_debit']:.4f}")
        print(f"  Reason:       {result['exit_reason']}")
        print(f"  Outcome row:  id={result['trade_outcome_id']}")
    except ValueError as e:
        print(f"✗ {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)
