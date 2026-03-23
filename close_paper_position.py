"""
close_paper_position.py — Paper position close script

Closes a PAPER iron condor position at market open mid-price.
Writes to trade_outcomes so Stage 2 eligibility tracking counts it.

Usage:
    uv run python close_paper_position.py --position-id 50 --mark-threshold 0.25

    --position-id     : positions.id to close
    --mark-threshold  : max allowed deviation from last mark as a fraction (default 0.25 = 25%)
                        If opening exit debit is > last_mark * (1 + threshold), pause and ask.
    --force           : skip the deviation check and submit regardless

Safety:
    - PAPER positions only — will refuse to run against LIVE or 5760/8096 account_ids
    - Reads live mid from option_quotes (latest snapshot) for each leg
    - Falls back to last mark if any leg mid is unavailable
    - Records closure in positions (status=closed), trade_decisions, and trade_outcomes
    - Does NOT touch live Schwab accounts
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── DB connection ─────────────────────────────────────────────────────────────
import os
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
    user=os.getenv("DB_USER", "postgres"),
    pw=os.getenv("DB_PASSWORD", "REDACTED"),
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    db=os.getenv("DB_NAME", "postgres"),
)


def _get_latest_snapshot_id(conn) -> Optional[int]:
    row = conn.execute(text("""
        SELECT id, ts FROM snapshot_runs
        WHERE status IN ('ok', 'partial')
        ORDER BY ts DESC LIMIT 1
    """)).fetchone()
    if row:
        logger.info(f"Latest snapshot: id={row.id} ts={row.ts}")
        return row.id
    return None


def _get_mid(conn, symbol, snapshot_id, expiry, strike, option_right) -> Optional[float]:
    row = conn.execute(text("""
        SELECT bid, ask FROM option_quotes
        WHERE symbol = :symbol
          AND snapshot_id = :snapshot_id
          AND expiry = :expiry
          AND strike = :strike
          AND option_right = :option_right
        LIMIT 1
    """), {
        "symbol": symbol, "snapshot_id": snapshot_id,
        "expiry": expiry, "strike": float(strike),
        "option_right": option_right,
    }).fetchone()
    if not row or row.bid is None or row.ask is None:
        return None
    return round((float(row.bid) + float(row.ask)) / 2.0, 4)


def close_paper_position(position_id: int, mark_threshold: float = 0.25, force: bool = False):
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        # ── 1. Load position ─────────────────────────────────────────────────
        pos = conn.execute(text("""
            SELECT id, account_id, symbol, strategy, expiry,
                   fill_credit, mark, quantity,
                   long_put_strike, short_put_strike,
                   short_call_strike, long_call_strike,
                   status, order_id
            FROM positions
            WHERE id = :id
        """), {"id": position_id}).fetchone()

        if not pos:
            logger.error(f"Position id={position_id} not found.")
            sys.exit(1)

        if pos.status != "open":
            logger.error(f"Position id={position_id} is already '{pos.status}' — aborting.")
            sys.exit(1)

        if pos.account_id != "PAPER":
            logger.error(
                f"SAFETY BLOCK: Position id={position_id} has account_id='{pos.account_id}'. "
                "This script only closes PAPER positions."
            )
            sys.exit(1)

        if pos.strategy != "IRON_CONDOR":
            logger.error(f"Position id={position_id} strategy='{pos.strategy}' — only IRON_CONDOR supported.")
            sys.exit(1)

        symbol       = pos.symbol
        expiry       = str(pos.expiry)
        fill_credit  = float(pos.fill_credit)
        last_mark    = float(pos.mark) if pos.mark is not None else fill_credit
        quantity     = int(pos.quantity) if pos.quantity else 3
        order_id_ref = pos.order_id

        logger.info(
            f"Closing PAPER {symbol} IC pos_id={position_id} "
            f"qty={quantity} fill_credit={fill_credit} last_mark={last_mark}"
        )

        # ── 2. Compute exit debit from live mids ─────────────────────────────
        snapshot_id = _get_latest_snapshot_id(conn)
        exit_debit  = None
        used_fallback = False

        if snapshot_id and all([
            pos.long_put_strike, pos.short_put_strike,
            pos.short_call_strike, pos.long_call_strike
        ]):
            lp  = _get_mid(conn, symbol, snapshot_id, expiry, pos.long_put_strike,   "P")
            sp  = _get_mid(conn, symbol, snapshot_id, expiry, pos.short_put_strike,  "P")
            sc  = _get_mid(conn, symbol, snapshot_id, expiry, pos.short_call_strike, "C")
            lc  = _get_mid(conn, symbol, snapshot_id, expiry, pos.long_call_strike,  "C")

            logger.info(f"Leg mids — LP:{lp} SP:{sp} SC:{sc} LC:{lc}")

            if all(x is not None for x in [lp, sp, sc, lc]):
                # Cost to close = buy back shorts, sell longs
                # debit = short_put + short_call - long_put - long_call  (what we pay)
                exit_debit = round(sp + sc - lp - lc, 4)
            else:
                logger.warning("One or more leg mids unavailable — falling back to last mark.")

        if exit_debit is None:
            exit_debit = last_mark
            used_fallback = True
            logger.warning(f"Using fallback exit_debit = last_mark = {last_mark}")

        # ── 3. Deviation check ────────────────────────────────────────────────
        deviation = (exit_debit - last_mark) / last_mark if last_mark > 0 else 0
        realized_pnl = round((fill_credit - exit_debit) * 100 * quantity, 4)

        print("\n" + "="*60)
        print(f"  CLOSE ORDER PREVIEW — {symbol} PAPER Iron Condor")
        print("="*60)
        print(f"  Position ID    : {position_id}")
        print(f"  Quantity       : {quantity} contracts")
        print(f"  Entry credit   : ${fill_credit:.4f}/contract")
        print(f"  Last mark      : ${last_mark:.4f}/contract")
        print(f"  Exit debit     : ${exit_debit:.4f}/contract {'[FALLBACK]' if used_fallback else '[live mids]'}")
        print(f"  Deviation      : {deviation:+.1%} vs last mark")
        print(f"  Realized P&L   : ${realized_pnl:+.2f}")
        print(f"  Profit %       : {(fill_credit - exit_debit) / fill_credit * 100:.1f}%")
        print("="*60)

        if not force and deviation > mark_threshold:
            print(f"\n⚠️  PAUSE: Exit debit ${exit_debit:.4f} is {deviation:+.1%} worse than last mark ${last_mark:.4f}.")
            print(f"   Threshold is {mark_threshold:.0%}. Re-run with --force to submit anyway.")
            logger.warning(f"Deviation {deviation:+.1%} exceeds threshold {mark_threshold:.0%} — pausing.")
            sys.exit(2)  # exit code 2 = pause/confirmation needed

        # ── 4. Write closure ──────────────────────────────────────────────────
        now = datetime.now(timezone.utc)

        # 4a. Mark position closed
        conn.execute(text("""
            UPDATE positions
            SET status     = 'closed',
                closed_at  = :now,
                close_reason = 'manual_paper_close',
                mark       = :exit_debit,
                unrealized_pnl = :realized_pnl
            WHERE id = :id
        """), {"now": now, "exit_debit": exit_debit, "realized_pnl": realized_pnl, "id": position_id})
        logger.info(f"positions id={position_id} marked closed")

        # 4b. Ensure trade_decisions row exists for the original candidate
        candidate_id = None
        if order_id_ref:
            ord_row = conn.execute(text(
                "SELECT candidate_id FROM orders WHERE id = :oid"
            ), {"oid": order_id_ref}).fetchone()
            if ord_row:
                candidate_id = ord_row.candidate_id

        decision_id = None
        if candidate_id:
            existing = conn.execute(text(
                "SELECT id FROM trade_decisions WHERE candidate_id = :cid"
            ), {"cid": candidate_id}).fetchone()
            if existing:
                decision_id = existing.id
                logger.info(f"Using existing trade_decisions id={decision_id} for candidate_id={candidate_id}")
            else:
                res = conn.execute(text("""
                    INSERT INTO trade_decisions (candidate_id, decision, reason, decided_at)
                    VALUES (:cid, 'approved', 'paper_fill', :now)
                    RETURNING id
                """), {"cid": candidate_id, "now": now})
                decision_id = res.scalar()
                logger.info(f"Inserted trade_decisions id={decision_id} for candidate_id={candidate_id}")

        # 4c. Insert trade_outcomes — this is what Stage 2 eligibility reads
        if decision_id:
            conn.execute(text("""
                INSERT INTO trade_outcomes (
                    decision_id, account_id, entry_credit, exit_debit,
                    fees, pnl, exit_reason, closed_at, notes
                ) VALUES (
                    :decision_id, 'PAPER', :entry_credit, :exit_debit,
                    0, :pnl, 'manual_paper_close', :now, :notes
                )
            """), {
                "decision_id":  decision_id,
                "entry_credit": fill_credit,
                "exit_debit":   exit_debit,
                "pnl":          realized_pnl,
                "now":          now,
                "notes":        f"Paper close at open — pos_id={position_id} qty={quantity}",
            })
            logger.info(f"trade_outcomes inserted — pnl={realized_pnl:.2f}")
        else:
            logger.warning(
                "No candidate_id resolved — trade_outcomes NOT inserted. "
                "Stage 2 tracking will not count this close."
            )

        print(f"\n✅ Close complete: pos_id={position_id} {symbol} PAPER")
        print(f"   Realized P&L recorded: ${realized_pnl:+.2f}")
        print(f"   trade_outcomes: {'recorded ✅' if decision_id else 'SKIPPED ⚠️ (no candidate link)'}")
        print(f"   Stage 2 eligible: {'yes' if decision_id else 'NO — recheck candidate_id link'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Close a PAPER iron condor position")
    parser.add_argument("--position-id", type=int, required=True)
    parser.add_argument("--mark-threshold", type=float, default=0.25,
                        help="Max deviation from last mark before pausing (default 0.25 = 25%%)")
    parser.add_argument("--force", action="store_true",
                        help="Skip deviation check and submit regardless")
    args = parser.parse_args()

    close_paper_position(
        position_id=args.position_id,
        mark_threshold=args.mark_threshold,
        force=args.force,
    )
