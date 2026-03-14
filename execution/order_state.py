"""
execution/order_state.py — Phase 5: Order Lifecycle Tracker

Responsibilities:
  1. migrate_orders_schema(engine) — idempotent DDL migration for orders + positions.
     Replaces the scattered CREATE TABLE IF NOT EXISTS blocks in executor.py,
     dry_run.py, and approval_ui/api.py.  Safe to call on startup every time.
  2. get_order_status(order_id)    — read current order row as a dict.
  3. update_order_status(...)      — write a status transition + optional fill_price.
  4. track_order(order_id)         — lifecycle tracker:
       paper: verify filled; force-fill via dry_run if still pending.
       live:  NotImplementedError — no Schwab polling code here.
  5. get_open_orders()             — returns all non-terminal orders.

TRADING_MODE=paper — this file contains no Schwab API calls.
Live tracking is intentionally stubbed.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text

from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    TRADING_MODE,
)

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Canonical order states ─────────────────────────────────────────────────────
PENDING   = "pending"
SUBMITTED = "submitted"
WORKING   = "working"
FILLED    = "filled"
REJECTED  = "rejected"
CANCELLED = "cancelled"

TERMINAL_STATES: frozenset[str] = frozenset({FILLED, REJECTED, CANCELLED})

# ── Phase 5 canonical DDL ─────────────────────────────────────────────────────
# Authoritative CREATE TABLE statements for the execution layer.
# executor.py and dry_run.py delegate schema ownership here.

_DDL_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    id            SERIAL PRIMARY KEY,
    candidate_id  INTEGER,
    account_id    TEXT,
    symbol        TEXT,
    status        TEXT        DEFAULT 'pending',
    source        TEXT        DEFAULT 'paper',
    order_payload JSONB,
    fill_price    NUMERIC,
    quantity      INTEGER,
    created_at    TIMESTAMPTZ DEFAULT now(),
    filled_at     TIMESTAMPTZ
);
"""

# The FK on order_id is only safe on a fresh table — the ALTER TABLE migration
# path below intentionally omits it because the pre-existing positions table
# may already have rows without order_id values.
_DDL_POSITIONS = """
CREATE TABLE IF NOT EXISTS positions (
    id                 SERIAL PRIMARY KEY,
    account_id         TEXT,
    symbol             TEXT,
    expiry             DATE,
    strategy           TEXT        DEFAULT 'IRON_CONDOR',
    long_put_strike    NUMERIC,
    short_put_strike   NUMERIC,
    short_call_strike  NUMERIC,
    long_call_strike   NUMERIC,
    quantity           INTEGER,
    fill_credit        NUMERIC,
    opened_at          TIMESTAMPTZ DEFAULT now(),
    status             TEXT        DEFAULT 'open',
    order_id           INTEGER     REFERENCES orders(id)
);
"""


# ── Migration ──────────────────────────────────────────────────────────────────

def migrate_orders_schema(engine) -> None:
    """
    Idempotent schema migration for the orders and positions tables.

    Strategy:
      1. CREATE TABLE IF NOT EXISTS — no-op if tables already exist.
      2. ALTER TABLE ADD COLUMN IF NOT EXISTS — adds Phase 5 columns to
         pre-existing tables created with the older schema (qty / created_ts /
         updated_ts column names).
      3. ALTER COLUMN — relax NOT NULL constraints on legacy columns that
         conflict with executor.py's INSERT statements, which write to the
         new column names (quantity, created_at, …) and leave old ones NULL.
         Guarded by a column-existence check so this is safe on fresh tables
         where those old columns were never created.

    Safe to call multiple times.  Never drops or renames existing columns.
    """
    # New columns required by executor.py / dry_run.py
    _orders_add: list[tuple[str, str]] = [
        ("candidate_id",  "INTEGER"),
        ("source",        "TEXT DEFAULT 'paper'"),
        ("order_payload", "JSONB"),
        ("fill_price",    "NUMERIC"),
        ("quantity",      "INTEGER"),
        ("created_at",    "TIMESTAMPTZ DEFAULT now()"),
        ("filled_at",     "TIMESTAMPTZ"),
    ]

    # order_id added without FK — see module docstring note.
    # source/close_reason added for reconciler.py (tracks how a position was opened/closed).
    _positions_add: list[tuple[str, str]] = [
        ("long_put_strike",   "NUMERIC"),
        ("short_put_strike",  "NUMERIC"),
        ("short_call_strike", "NUMERIC"),
        ("long_call_strike",  "NUMERIC"),
        ("quantity",          "INTEGER"),
        ("fill_credit",       "NUMERIC"),
        ("order_id",          "INTEGER"),
        ("source",            "TEXT"),          # 'paper' | 'manual' | 'executor'
        ("close_reason",      "TEXT"),          # 'manual_or_expired' | 'exit_signal' | …
    ]

    # Legacy NOT NULL columns that conflict with Phase 5 writes.
    # (table, column, ALTER COLUMN fragment)
    _relax: list[tuple[str, str, str]] = [
        ("orders",    "qty",        "SET DEFAULT 0"),
        ("orders",    "qty",        "DROP NOT NULL"),
        ("orders",    "status",     "SET DEFAULT 'pending'"),
        ("orders",    "account_id", "SET DEFAULT 'PAPER'"),
        ("orders",    "symbol",     "DROP NOT NULL"),
        ("positions", "qty",        "SET DEFAULT 0"),
        ("positions", "qty",        "DROP NOT NULL"),
        ("positions", "account_id", "SET DEFAULT 'PAPER'"),
        ("positions", "symbol",     "DROP NOT NULL"),
        ("positions", "opened_ts",  "SET DEFAULT now()"),
    ]

    with engine.begin() as conn:
        # Step 1 — ensure tables exist (no-op on existing)
        conn.execute(text(_DDL_ORDERS))
        conn.execute(text(_DDL_POSITIONS))

        # Step 2 — add missing Phase 5 columns
        for col, coltype in _orders_add:
            conn.execute(text(
                f"ALTER TABLE orders ADD COLUMN IF NOT EXISTS {col} {coltype}"
            ))
        for col, coltype in _positions_add:
            conn.execute(text(
                f"ALTER TABLE positions ADD COLUMN IF NOT EXISTS {col} {coltype}"
            ))

        # Step 3 — relax legacy NOT NULL constraints, guarded by existence check
        existing: dict[str, set[str]] = {}
        for table in ("orders", "positions"):
            rows = conn.execute(text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = :t
            """), {"t": table}).fetchall()
            existing[table] = {r[0] for r in rows}

        for table, col, action in _relax:
            if col in existing[table]:
                conn.execute(text(
                    f"ALTER TABLE {table} ALTER COLUMN {col} {action}"
                ))

        # Unique index on positions.position_key — required for reconciler's
        # ON CONFLICT (position_key) DO NOTHING INSERT.
        # NULL values do not conflict with each other in a UNIQUE index (PostgreSQL
        # semantics), so this is safe even for rows that pre-date position_key.
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_position_key
            ON positions (position_key)
            WHERE position_key IS NOT NULL
        """))

    logger.info("migrate_orders_schema: orders + positions schema verified/migrated")


# ── Read helpers ───────────────────────────────────────────────────────────────

def get_order_status(order_id: int) -> dict:
    """
    Returns the current orders row as a plain dict.

    Keys: id, candidate_id, account_id, symbol, status, source,
          fill_price, quantity, created_at, filled_at.
    Datetime values are converted to ISO-8601 strings.

    Raises ValueError if the order is not found.
    """
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, candidate_id, account_id, symbol,
                   status, source, fill_price, quantity,
                   created_at, filled_at
            FROM orders
            WHERE id = :id
        """), {"id": order_id}).fetchone()

    if not row:
        raise ValueError(f"get_order_status: order id={order_id} not found")

    d = dict(row._mapping)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    return d


def get_open_orders() -> list[dict]:
    """
    Returns all orders whose status is not in TERMINAL_STATES
    (i.e. not filled, rejected, or cancelled), ordered oldest-first.
    """
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, candidate_id, account_id, symbol,
                   status, source, fill_price, quantity,
                   created_at, filled_at
            FROM orders
            WHERE status NOT IN ('filled', 'rejected', 'cancelled')
            ORDER BY created_at ASC NULLS LAST
        """)).fetchall()

    result = []
    for row in rows:
        d = dict(row._mapping)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        result.append(d)
    return result


# ── Write helpers ──────────────────────────────────────────────────────────────

def update_order_status(
    order_id: int,
    new_status: str,
    fill_price: Optional[float] = None,
) -> None:
    """
    Writes a status transition to the orders row and logs it.

    When new_status == FILLED and fill_price is provided, also sets
    fill_price and filled_at = now().

    Raises ValueError if the order is not found.
    """
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT status FROM orders WHERE id = :id"),
            {"id": order_id},
        ).fetchone()
        if not row:
            raise ValueError(f"update_order_status: order id={order_id} not found")
        old_status = row.status

        if new_status == FILLED and fill_price is not None:
            conn.execute(text("""
                UPDATE orders
                SET status     = :status,
                    fill_price = :fill_price,
                    filled_at  = :filled_at
                WHERE id = :id
            """), {
                "status":     new_status,
                "fill_price": fill_price,
                "filled_at":  datetime.now(timezone.utc),
                "id":         order_id,
            })
        else:
            conn.execute(text("""
                UPDATE orders SET status = :status WHERE id = :id
            """), {"status": new_status, "id": order_id})

    logger.info(
        f"update_order_status: order_id={order_id} "
        f"{old_status!r} → {new_status!r}"
        + (f" fill_price={fill_price}" if fill_price is not None else "")
    )


# ── Lifecycle tracker ──────────────────────────────────────────────────────────

def track_order(order_id: int) -> None:
    """
    Main lifecycle tracker.  Behaviour depends on TRADING_MODE:

    paper:
      In paper mode, dry_run.simulate_fill runs synchronously inside
      execute_approved_candidate, so orders should already be 'filled'
      by the time track_order is called.

      If the order is already in a terminal state → logs and returns.

      If the order is still non-terminal (e.g. a crash between executor's
      INSERT and dry_run's UPDATE) → loads candidate_json and calls
      dry_run.simulate_fill to force the fill, ensuring the lifecycle
      loop always closes cleanly.

    live:
      Raises NotImplementedError — Schwab order-status polling is not
      implemented.  Complete Phase 5 live sign-off before enabling.

    Raises:
      ValueError           — order not found or candidate data missing
      NotImplementedError  — TRADING_MODE=live
    """
    if TRADING_MODE == "live":
        raise NotImplementedError(
            "Live order tracking not enabled. "
            "Complete Phase 5 live sign-off first."
        )

    # ── Paper path ─────────────────────────────────────────────────────────────
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, status, candidate_id, quantity, symbol
            FROM orders
            WHERE id = :id
        """), {"id": order_id}).fetchone()

    if not row:
        raise ValueError(f"track_order: order id={order_id} not found")

    current_status = row.status
    symbol         = row.symbol or "?"

    logger.info(
        f"track_order: order_id={order_id} symbol={symbol} "
        f"status={current_status!r} mode=paper"
    )

    # Already terminal — nothing to do
    if current_status in TERMINAL_STATES:
        logger.info(
            f"track_order: order_id={order_id} already in terminal state "
            f"{current_status!r} — done"
        )
        return

    # Non-terminal paper order: dry_run should have filled this synchronously.
    # If we're here, something went wrong between executor INSERT and dry_run UPDATE.
    # Force-fill to close the lifecycle loop.
    logger.warning(
        f"track_order: paper order_id={order_id} status={current_status!r} "
        f"— expected 'filled' after simulate_fill; forcing fill now"
    )

    candidate_id = row.candidate_id
    quantity     = row.quantity or 1

    if candidate_id is None:
        raise ValueError(
            f"track_order: cannot force-fill order_id={order_id} "
            "— candidate_id is NULL (order was not created by execute_approved_candidate)"
        )

    with engine.connect() as conn:
        tc_row = conn.execute(text("""
            SELECT candidate_json FROM trade_candidates WHERE id = :id
        """), {"id": candidate_id}).fetchone()

    if not tc_row:
        raise ValueError(
            f"track_order: cannot force-fill order_id={order_id} "
            f"— trade_candidate id={candidate_id} not found"
        )

    candidate_json = (
        tc_row.candidate_json
        if isinstance(tc_row.candidate_json, dict)
        else json.loads(tc_row.candidate_json or "{}")
    )

    from execution.dry_run import simulate_fill  # noqa: PLC0415

    simulate_fill(candidate_json, quantity, order_id)
    logger.info(
        f"track_order: force-fill complete — order_id={order_id} symbol={symbol}"
    )
