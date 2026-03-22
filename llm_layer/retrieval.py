"""
llm_layer/retrieval.py

DB-only context retrieval for trade cards.
Must NOT import trade_card (avoid circular imports).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from loguru import logger
from sqlalchemy import create_engine, text

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def _get_columns(conn, table: str) -> set[str]:
    rows = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:t
    """), {"t": table}).fetchall()
    return {r[0] for r in rows}


def build_context_block(symbol: str, account_id: Optional[str] = None, lookback_days: int = 30) -> str:
    """
    Return a compact newline-delimited context block.
    Best-effort: never raises.
    """
    lines: list[str] = []
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=lookback_days)

    try:
        engine = create_engine(DB_URL, pool_pre_ping=True)

        with engine.connect() as conn:
            if account_id:
                lines.append(f"Account focus: {account_id}")

            # Open positions in same symbol (all strategies — matches Positions tab)
            pos = conn.execute(text("""
                SELECT account_id, position_key, strategy, expiry, dte,
                       COALESCE(fill_credit, entry_credit) AS credit
                FROM positions
                WHERE status = 'open' AND symbol = :sym
                ORDER BY dte ASC NULLS LAST
                LIMIT 5
            """), {"sym": symbol}).fetchall()

            if pos:
                lines.append("Open positions (same symbol):")
                for r in pos:
                    strat = r[2] or "UNKNOWN"
                    lines.append(
                        f"- acct={r[0]} strategy={strat} key={r[1]} "
                        f"exp={r[3]} dte={r[4]} credit={r[5]}"
                    )
            else:
                lines.append("Open positions (same symbol): none")

            # Recent exit signals
            sig = conn.execute(text("""
                SELECT created_at, reason, pnl_pct, dte, status
                FROM exit_signals
                WHERE symbol=:sym AND created_at >= :since
                ORDER BY created_at DESC
                LIMIT 5
            """), {"sym": symbol, "since": since}).fetchall()

            if sig:
                lines.append(f"Recent exit signals (last {lookback_days}d):")
                for r in sig:
                    lines.append(f"- {r[0].isoformat()} reason={r[1]} pnl_pct={r[2]} dte={r[3]} status={r[4]}")
            else:
                lines.append(f"Recent exit signals (last {lookback_days}d): none")

            # Latest IV/price context if columns exist
            uq_cols = _get_columns(conn, "underlying_quotes")
            ts_col = "ts" if "ts" in uq_cols else ("time" if "time" in uq_cols else None)

            if ts_col:
                parts = [ts_col]
                for c in ("price", "iv_pct", "iv_rank"):
                    if c in uq_cols:
                        parts.append(c)

                row = conn.execute(text(f"""
                    SELECT {", ".join(parts)}
                    FROM underlying_quotes
                    WHERE symbol=:sym
                    ORDER BY {ts_col} DESC
                    LIMIT 1
                """), {"sym": symbol}).fetchone()

                if row:
                    lines.append(f"Latest underlying quote: {dict(row._mapping)}")

    except Exception as e:
        logger.exception(f"build_context_block failed for {symbol}: {e}")
        lines.append("Context retrieval error (see logs).")

    return "\n".join(lines)