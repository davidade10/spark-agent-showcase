"""
strategy_engine/exit_monitor.py — Exit Signal Generation

Runs every 15 minutes during market hours (wired into main.py scheduler).

Pipeline:
  1. compute_position_marks(engine)  — look up current mid-prices from the
     latest option_quotes snapshot and compute the spread mark for each
     open position.
  2. run_exit_scan(engine)           — evaluate all 7 trigger rules against
     current marks; insert de-duped exit_signals rows; update positions.mark.
  3. dismiss_expired_signals(engine) — auto-dismiss signals for closed positions.
  4. clear_stale_signals(engine, marks) — dismiss pending signals whose
     trigger condition is no longer met (mark moved away from threshold).

Signal trigger rules:
  PROFIT_TARGET    mark <= fill_credit * 0.50  → info
  STRONG_CLOSE     mark <= fill_credit * 0.25  → info
  APPROACHING_STOP mark >= fill_credit * 2.00  → warning
  STOP_LOSS        mark >= fill_credit * 3.00  → critical
  TIME_EXIT_WARN   DTE <= 21 AND pnl_pct < 30% → info
  TIME_EXIT_CRITICAL DTE <= 7                  → warning
  GAMMA_RISK       DTE <= 7                    → critical

mark sign convention matches fill_credit:
  IRON_CONDOR — mark = short_put_mid + short_call_mid - long_put_mid - long_call_mid
  SHORT_OPTION — mark = short_leg_mid  (positive = cost to close)
  LONG_OPTION  — mark = long_leg_mid   (positive = current value)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ── Mark computation ───────────────────────────────────────────────────────────

def compute_position_marks(engine) -> dict[int, Optional[float]]:
    """
    Compute the current mid-price mark for every open position using the
    latest option_quotes snapshot.

    Returns {position_id: mark} where mark is None if any required leg is
    absent from option_quotes (don't guess — return None so the caller can
    skip signal generation for that position).

    Mark sign convention (matches fill_credit — positive = premium received):
      IRON_CONDOR  → short_put_mid + short_call_mid − long_put_mid − long_call_mid
      SHORT_OPTION → short_leg_mid
      LONG_OPTION  → long_leg_mid
      Others       → None (EQUITY has no option mark)
    """
    with engine.connect() as conn:
        # Latest completed snapshot
        snap_row = conn.execute(text("""
            SELECT id FROM snapshot_runs
            WHERE status IN ('ok', 'partial')
            ORDER BY id DESC LIMIT 1
        """)).fetchone()
        if not snap_row:
            logger.info("exit_monitor.compute_position_marks: no completed snapshots yet")
            return {}
        snapshot_id = snap_row[0]

        # All open positions with their strike columns
        pos_rows = conn.execute(text("""
            SELECT id, symbol, expiry, strategy,
                   long_put_strike, short_put_strike,
                   short_call_strike, long_call_strike
            FROM positions
            WHERE status = 'open'
        """)).fetchall()
        if not pos_rows:
            return {}

        # Batch-fetch option_quotes for all relevant symbols from this snapshot
        symbols = list({r[1] for r in pos_rows})
        quote_rows = conn.execute(text("""
            SELECT symbol,
                   CAST(expiry AS TEXT) AS expiry_text,
                   option_right,
                   CAST(strike AS FLOAT) AS strike,
                   bid, ask
            FROM option_quotes
            WHERE snapshot_id = :sid
              AND symbol = ANY(:symbols)
              AND bid IS NOT NULL
              AND ask IS NOT NULL
        """), {"sid": snapshot_id, "symbols": symbols}).fetchall()

        # Build lookup: (symbol, expiry_date_str, option_right, strike) → mid
        quote_map: dict[tuple, float] = {}
        for qr in quote_rows:
            sym, exp_text, right, strike, bid, ask = qr
            mid = (float(bid) + float(ask)) / 2.0
            key = (sym, str(exp_text)[:10], right, round(float(strike), 4))
            quote_map[key] = mid

    # Compute mark per position
    marks: dict[int, Optional[float]] = {}
    for row in pos_rows:
        pos_id   = row[0]
        sym      = row[1]
        exp      = str(row[2])[:10] if row[2] is not None else None
        strategy = (row[3] or "IRON_CONDOR").upper()
        lp  = round(float(row[4]), 4) if row[4] is not None else None
        sp  = round(float(row[5]), 4) if row[5] is not None else None
        sc  = round(float(row[6]), 4) if row[6] is not None else None
        lc  = round(float(row[7]), 4) if row[7] is not None else None

        if not exp:
            marks[pos_id] = None
            continue

        if strategy == "IRON_CONDOR":
            if any(x is None for x in (lp, sp, sc, lc)):
                marks[pos_id] = None
                continue
            lp_mid = quote_map.get((sym, exp, "P", lp))
            sp_mid = quote_map.get((sym, exp, "P", sp))
            sc_mid = quote_map.get((sym, exp, "C", sc))
            lc_mid = quote_map.get((sym, exp, "C", lc))
            if None in (lp_mid, sp_mid, sc_mid, lc_mid):
                logger.debug(
                    f"exit_monitor: missing quote leg(s) for {sym} {exp} "
                    f"lp={lp_mid} sp={sp_mid} sc={sc_mid} lc={lc_mid}"
                )
                marks[pos_id] = None
            else:
                marks[pos_id] = round(sp_mid + sc_mid - lp_mid - lc_mid, 4)

        elif strategy == "SHORT_OPTION":
            strike = sc if sc is not None else sp
            right  = "C" if sc is not None else "P"
            if strike is None:
                marks[pos_id] = None
            else:
                mid = quote_map.get((sym, exp, right, strike))
                marks[pos_id] = round(mid, 4) if mid is not None else None

        elif strategy == "LONG_OPTION":
            strike = lc if lc is not None else lp
            right  = "C" if lc is not None else "P"
            if strike is None:
                marks[pos_id] = None
            else:
                mid = quote_map.get((sym, exp, right, strike))
                marks[pos_id] = round(mid, 4) if mid is not None else None

        else:
            # EQUITY, STRANGLE, STRADDLE, VERTICAL_SPREAD, UNKNOWN — no single mark
            marks[pos_id] = None

    return marks


# ── Trigger evaluation ────────────────────────────────────────────────────────

_SEVERITY: dict[str, str] = {
    "PROFIT_TARGET":      "info",
    "STRONG_CLOSE":       "info",
    "APPROACHING_STOP":   "warning",
    "STOP_LOSS":          "critical",
    "TIME_EXIT_WARN":     "info",
    "TIME_EXIT_CRITICAL": "warning",
    "GAMMA_RISK":         "critical",
}

_MESSAGE: dict[str, str] = {
    "PROFIT_TARGET":      "50% target reached — close for profit",
    "STRONG_CLOSE":       "75% of max profit — strong close, diminishing theta",
    "APPROACHING_STOP":   "Approaching stop — unrealized loss > 100% of credit",
    "STOP_LOSS":          "STOP TRIGGERED — close immediately",
    "TIME_EXIT_WARN":     "Time decay concern — monitor closely",
    "TIME_EXIT_CRITICAL": "Expiration week — close or roll unless near max profit",
    "GAMMA_RISK":         "CRITICAL: gamma risk escalation at 7 DTE",
}


def _eval_triggers(
    mark: float, fill_credit: float, dte: Optional[int], pnl_pct: float
) -> list[str]:
    """Return list of reason codes whose trigger conditions are currently met."""
    triggered: list[str] = []
    if mark <= fill_credit * 0.50:
        triggered.append("PROFIT_TARGET")
    if mark <= fill_credit * 0.25:
        triggered.append("STRONG_CLOSE")
    if mark >= fill_credit * 2.00:
        triggered.append("APPROACHING_STOP")
    if mark >= fill_credit * 3.00:
        triggered.append("STOP_LOSS")
    if dte is not None:
        if dte <= 21 and pnl_pct < 30.0:
            triggered.append("TIME_EXIT_WARN")
        if dte <= 7:
            triggered.append("TIME_EXIT_CRITICAL")
        if dte <= 7:
            triggered.append("GAMMA_RISK")
    return triggered


def _is_still_triggered(
    reason: str, mark: float, fill_credit: float, dte: Optional[int], pnl_pct: float
) -> bool:
    """True if the trigger condition for *reason* still applies at current mark."""
    triggered = _eval_triggers(mark, fill_credit, dte, pnl_pct)
    return reason in triggered


# ── Core scan ─────────────────────────────────────────────────────────────────

def run_exit_scan(engine=None) -> list[dict]:
    """
    Main entry point — scan all open positions and generate exit signals.

    Steps:
      1. compute_position_marks — get current marks from latest snapshot
      2. dismiss_expired_signals — tidy up signals for closed positions
      3. clear_stale_signals — remove pending signals that are no longer triggered
      4. For each position with a valid mark, evaluate all 7 rules and insert
         de-duped signals into exit_signals

    Returns list of newly created signal dicts.
    """
    if engine is None:
        engine = create_engine(DB_URL)

    marks = compute_position_marks(engine)
    logger.info(
        f"exit_monitor: marks computed for {sum(1 for v in marks.values() if v is not None)} "
        f"of {len(marks)} open positions"
    )

    dismiss_expired_signals(engine)
    clear_stale_signals(engine, marks)

    now = datetime.now(timezone.utc)
    new_signals: list[dict] = []

    with engine.begin() as conn:
        pos_rows = conn.execute(text("""
            SELECT id, account_id, symbol, expiry, strategy, dte,
                   fill_credit, quantity, position_key
            FROM positions
            WHERE status = 'open'
        """)).fetchall()

        for row in pos_rows:
            pos_id      = row[0]
            symbol      = row[2]
            expiry      = row[3]
            dte         = row[5]
            fill_credit = float(row[6]) if row[6] is not None else None
            quantity    = int(row[7]) if row[7] is not None else 1

            mark = marks.get(pos_id)

            # Always update mark on the position row if we have one
            if mark is not None:
                conn.execute(text("""
                    UPDATE positions
                    SET mark = :mark, mark_updated_at = :now
                    WHERE id = :id
                """), {"mark": mark, "now": now, "id": pos_id})

            if mark is None:
                logger.debug(
                    f"exit_monitor: no mark available for {symbol} pos_id={pos_id} — skipping"
                )
                continue

            if fill_credit is None or fill_credit <= 0:
                logger.debug(
                    f"exit_monitor: no fill_credit for {symbol} pos_id={pos_id} — skipping"
                )
                continue

            pnl_pct     = ((fill_credit - mark) / fill_credit) * 100.0
            pnl_dollars = (fill_credit - mark) * quantity * 100.0

            for reason in _eval_triggers(mark, fill_credit, dte, pnl_pct):
                # Dedupe: skip if same position + reason is already active
                existing = conn.execute(text("""
                    SELECT 1 FROM exit_signals
                    WHERE position_id = :pid
                      AND reason = :reason
                      AND (
                            status = 'pending'
                         OR (status = 'snoozed' AND snoozed_until > NOW())
                      )
                    LIMIT 1
                """), {"pid": pos_id, "reason": reason}).fetchone()
                if existing:
                    continue

                result = conn.execute(text("""
                    INSERT INTO exit_signals (
                        position_id, symbol, expiry, dte,
                        reason, severity, message,
                        pnl_pct, pnl_dollars,
                        credit_received, debit_to_close, mark,
                        status, created_at, updated_at
                    ) VALUES (
                        :position_id, :symbol, :expiry, :dte,
                        :reason, :severity, :message,
                        :pnl_pct, :pnl_dollars,
                        :credit_received, :debit_to_close, :mark,
                        'pending', :now, :now
                    )
                    RETURNING id
                """), {
                    "position_id":   pos_id,
                    "symbol":        symbol,
                    "expiry":        expiry,
                    "dte":           dte,
                    "reason":        reason,
                    "severity":      _SEVERITY[reason],
                    "message":       _MESSAGE[reason],
                    "pnl_pct":       round(pnl_pct, 2),
                    "pnl_dollars":   round(pnl_dollars, 2),
                    "credit_received": fill_credit,
                    "debit_to_close":  round(mark, 4),
                    "mark":            round(mark, 4),
                    "now":             now,
                })
                new_id = result.fetchone()[0]
                new_signals.append({
                    "id":          new_id,
                    "position_id": pos_id,
                    "symbol":      symbol,
                    "reason":      reason,
                    "severity":    _SEVERITY[reason],
                    "pnl_pct":     round(pnl_pct, 2),
                })
                logger.info(
                    f"exit_monitor: signal id={new_id} "
                    f"{symbol} reason={reason} severity={_SEVERITY[reason]} "
                    f"mark={mark:.4f} fill_credit={fill_credit:.4f} "
                    f"pnl_pct={pnl_pct:.1f}% dte={dte}"
                )

    count = len(new_signals)
    if count:
        logger.info(f"exit_monitor: scan complete — {count} new signal(s) generated")
    else:
        logger.info("exit_monitor: scan complete — no new signals")

    return new_signals


# ── Maintenance helpers ───────────────────────────────────────────────────────

def dismiss_expired_signals(engine) -> int:
    """
    Mark exit_signals as 'dismissed' when their underlying position is closed.
    Called automatically at the start of each run_exit_scan cycle.
    Returns the number of rows updated.
    """
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE exit_signals es
            SET status = 'dismissed', updated_at = NOW()
            WHERE es.status IN ('pending', 'acknowledged', 'snoozed')
              AND es.position_id IS NOT NULL
              AND (
                  SELECT p.status FROM positions p WHERE p.id = es.position_id
              ) = 'closed'
        """))
        count = result.rowcount
    if count:
        logger.info(f"exit_monitor: dismissed {count} signal(s) for closed positions")
    return count


def clear_stale_signals(engine, marks: dict[int, Optional[float]]) -> int:
    """
    Dismiss pending signals whose trigger condition is no longer met.

    Only 'pending' signals are cleared — 'acknowledged' and 'snoozed' ones
    are left for the user to act on even if the mark has moved.
    """
    dismissed = 0
    with engine.begin() as conn:
        pending_rows = conn.execute(text("""
            SELECT es.id, es.reason, es.position_id,
                   p.fill_credit, p.dte
            FROM exit_signals es
            JOIN positions p ON p.id = es.position_id
            WHERE es.status = 'pending'
        """)).fetchall()

        for sig_id, reason, pos_id, fill_credit_raw, dte in pending_rows:
            mark        = marks.get(pos_id)
            fill_credit = float(fill_credit_raw) if fill_credit_raw is not None else None

            if mark is None or fill_credit is None or fill_credit <= 0:
                continue

            pnl_pct = ((fill_credit - mark) / fill_credit) * 100.0
            if not _is_still_triggered(reason, mark, fill_credit, dte, pnl_pct):
                conn.execute(text("""
                    UPDATE exit_signals
                    SET status = 'dismissed', updated_at = NOW()
                    WHERE id = :id
                """), {"id": sig_id})
                dismissed += 1
                logger.info(
                    f"exit_monitor: cleared stale signal id={sig_id} "
                    f"reason={reason} (trigger no longer met)"
                )

    return dismissed


# ── Standalone entry point ────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    logger.info("Running exit monitor scan...")
    signals = run_exit_scan()
    logger.info(f"Done — {len(signals)} new signal(s) generated")
