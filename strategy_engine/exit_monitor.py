"""
strategy_engine/exit_monitor.py

Runs alongside the collector (every 15 minutes).
Checks all 'open' positions in the database against three strict quantitative rules:
1. Profit Target: 50% of entry credit
2. Stop Loss: 200% of entry credit (cost to close is 2x credit received)
3. Time Stop: <= 21 DTE (gamma risk acceleration)

Generates exit signals in exit_signals table for the Approval UI.
"""

import json
import logging
from sqlalchemy import create_engine, text
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

EXIT_RULES = {
    "profit_target_pct": 0.50,
    "stop_loss_multiple": 2.0,
    "dte_close": 21,
}

DEDUPE_WINDOW_HOURS = 6  # don't generate same signal repeatedly every run


def _meta_dict(meta) -> dict:
    """Normalize meta to a dict (JSONB may come back as dict, str, or None)."""
    if meta is None:
        return {}
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, str):
        try:
            return json.loads(meta)
        except Exception:
            return {}
    return {}


def _dedupe_exists(conn, position_key: str, reason: str) -> bool:
    row = conn.execute(
        text(
            """
            SELECT 1
            FROM exit_signals
            WHERE position_key = :pk
              AND reason = :reason
              AND status IN ('new','acknowledged')
              AND created_at > NOW() - (:hours || ' hours')::interval
            LIMIT 1
            """
        ),
        {"pk": position_key, "reason": reason, "hours": DEDUPE_WINDOW_HOURS},
    ).fetchone()
    return row is not None


def check_exit_signals(position: dict) -> dict | None:
    """
    Evaluates a single position dict against the exit rules.
    Returns a signal dict if triggered, otherwise None.
    """
    symbol = position["symbol"]
    entry_credit = position.get("entry_credit")
    dte = position.get("dte")

    meta = _meta_dict(position.get("meta"))
    debit_to_close = meta.get("debit_to_close")

    # Must have all 3 to evaluate safely
    try:
        entry_credit = float(entry_credit)
        debit_to_close = float(debit_to_close)
        dte = int(dte)
    except Exception:
        return None

    if entry_credit <= 0:
        return None

    pnl = entry_credit - debit_to_close
    pnl_pct = pnl / entry_credit

    # 1) Time stop (gamma risk)
    if dte <= EXIT_RULES["dte_close"]:
        logger.info(f"[{symbol}] EXIT SIGNAL: Time stop triggered at {dte} DTE.")
        return {"reason": "time_exit", "pnl_pct": pnl_pct, "dte": dte, "pnl": pnl, "debit": debit_to_close}

    # 2) Take profit
    if pnl_pct >= EXIT_RULES["profit_target_pct"]:
        logger.info(f"[{symbol}] EXIT SIGNAL: Profit target hit! P&L: {pnl_pct:.1%}")
        return {"reason": "take_profit", "pnl_pct": pnl_pct, "dte": dte, "pnl": pnl, "debit": debit_to_close}

    # 3) Stop loss (cost-to-close >= 2x credit)
    if debit_to_close >= (entry_credit * EXIT_RULES["stop_loss_multiple"]):
        logger.warning(
            f"[{symbol}] EXIT SIGNAL: Stop loss breached! Close=${debit_to_close:.2f} vs Credit=${entry_credit:.2f}"
        )
        return {"reason": "stop_loss", "pnl_pct": pnl_pct, "dte": dte, "pnl": pnl, "debit": debit_to_close}

    return None


def run_exit_monitor():
    """
    Main loop: Scans DB for open positions, runs the checks, inserts signals.
    """
    engine = create_engine(DB_URL)
    signals_generated = 0

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, account_id, position_key, symbol, entry_credit, dte, expiry, meta
                FROM positions
                WHERE status = 'open' AND strategy = 'iron_condor' AND position_key IS NOT NULL
                """
            )
        ).fetchall()

        if not rows:
            logger.info("Exit Monitor: No open positions to track.")
            return

        for r in rows:
            position_data = {
                "id": r[0],
                "account_id": r[1],
                "position_key": r[2],
                "symbol": r[3],
                "entry_credit": r[4],
                "dte": r[5],
                "expiry": r[6],
                "meta": r[7],
            }

            signal = check_exit_signals(position_data)
            if not signal:
                continue

            # Dedupe repeated signals
            if _dedupe_exists(conn, position_data["position_key"], signal["reason"]):
                continue

            # Insert to exit_signals (Approval UI reads this)
            conn.execute(
                text(
                    """
                    INSERT INTO exit_signals (
                      account_id, position_key, symbol, expiry, reason,
                      credit_received, debit_to_close, pnl_dollars, pnl_pct, dte,
                      status, meta
                    )
                    VALUES (
                      :account_id, :position_key, :symbol, :expiry, :reason,
                      :credit, :debit, :pnl, :pnl_pct, :dte,
                      'new', CAST(:meta AS JSONB)
                    )
                    """
                ),
                {
                    "account_id": position_data["account_id"],
                    "position_key": position_data["position_key"],
                    "symbol": position_data["symbol"],
                    "expiry": position_data["expiry"],
                    "reason": signal["reason"],
                    "credit": float(position_data["entry_credit"]),
                    "debit": float(signal["debit"]),
                    "pnl": float(signal["pnl"]),
                    "pnl_pct": float(signal["pnl_pct"]),
                    "dte": int(signal["dte"]),
                    "meta": json.dumps({"source": "exit_monitor", "note": "auto-generated"}),
                },
            )

            signals_generated += 1

    if signals_generated > 0:
        logger.info(f"Exit Monitor complete: {signals_generated} action(s) required.")
    else:
        logger.info("Exit Monitor complete: no actions triggered.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    print("Running Exit Monitor scan...\n")
    run_exit_monitor()