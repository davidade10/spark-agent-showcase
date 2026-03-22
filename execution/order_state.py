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

PARTIAL_FILL = "partial_fill"
FAILED       = "failed"

TERMINAL_STATES: frozenset[str] = frozenset({FILLED, REJECTED, CANCELLED, FAILED})

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
        # Live execution columns — added Part 3
        ("schwab_order_id", "TEXT"),   # order ID from Schwab Location header
        ("error_message",   "TEXT"),   # failure reason if status='failed'
    ]

    # order_id added without FK — see module docstring note.
    # source/close_reason added for reconciler.py (tracks how a position was opened/closed).
    _positions_add: list[tuple[str, str]] = [
        ("long_put_strike",      "NUMERIC"),
        ("short_put_strike",     "NUMERIC"),
        ("short_call_strike",    "NUMERIC"),
        ("long_call_strike",     "NUMERIC"),
        ("quantity",             "INTEGER"),
        ("fill_credit",          "NUMERIC"),
        ("unrealized_pnl",       "NUMERIC"),
        ("order_id",             "INTEGER"),
        ("source",               "TEXT"),          # 'paper' | 'manual' | 'executor'
        ("close_reason",         "TEXT"),          # 'manual_or_expired' | 'exit_signal' | …
        # Closure-hardening columns (Phase 5 safety)
        ("closure_strikes",      "INTEGER DEFAULT 0"),  # consecutive absences counter
        ("last_seen_in_schwab",  "TIMESTAMPTZ"),         # last confirmed-live timestamp
        # Strategy generalisation (non-condor positions)
        ("legs_json",            "JSONB"),               # raw leg data for non-standard strategies
        # Exit monitor — live mark price
        ("mark",                 "NUMERIC"),             # current spread mid-price
        ("mark_updated_at",      "TIMESTAMPTZ"),         # when mark was last computed
    ]

    # exit_signals — new columns added to potentially-existing table
    _exit_signals_add: list[tuple[str, str]] = [
        ("position_id",    "INTEGER"),
        ("account_id",     "TEXT"),
        ("severity",       "TEXT DEFAULT 'info'"),
        ("mark",           "NUMERIC"),
        ("message",        "TEXT"),
        ("snoozed_until",  "TIMESTAMPTZ"),
        ("updated_at",     "TIMESTAMPTZ DEFAULT now()"),
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

        # exit_signals — one row per triggered exit condition per position.
        # Uses status: 'pending' | 'acknowledged' | 'snoozed' | 'dismissed'
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS exit_signals (
                id              SERIAL PRIMARY KEY,
                position_id     INTEGER REFERENCES positions(id),
                account_id      TEXT,
                symbol          TEXT,
                expiry          DATE,
                dte             INTEGER,
                reason          TEXT,
                severity        TEXT        DEFAULT 'info',
                pnl_pct         NUMERIC,
                pnl_dollars     NUMERIC,
                credit_received NUMERIC,
                debit_to_close  NUMERIC,
                mark            NUMERIC,
                message         TEXT,
                status          TEXT        DEFAULT 'pending',
                snoozed_until   TIMESTAMPTZ,
                created_at      TIMESTAMPTZ DEFAULT now(),
                updated_at      TIMESTAMPTZ DEFAULT now()
            )
        """))

        # Migrate existing exit_signals rows (table may pre-date this schema)
        for col, coltype in _exit_signals_add:
            conn.execute(text(
                f"ALTER TABLE exit_signals ADD COLUMN IF NOT EXISTS {col} {coltype}"
            ))

        # Idempotency safety:
        # We only want to block duplicate *active* signals, not all future signals forever.
        #
        # Active states in this codebase:
        # - 'pending' is always active until dismissed/acknowledged/snoozed.
        # - 'snoozed' is conditionally active (snoozed_until > NOW()) and is handled
        #   by the Python pre-check in exit_monitor (time-based partial index would be brittle).
        #
        # Therefore, enforce uniqueness only for the always-active DB state: status='pending'.
        conn.execute(text("DROP INDEX IF EXISTS idx_exit_signals_position_reason"))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_exit_signals_position_reason_pending
            ON exit_signals (position_id, reason)
            WHERE status = 'pending'
        """))

        # trade_candidates — blocked_reason column for shadow mode.
        # Stores the failing rule name and human-readable detail for blocked
        # candidates so GET /shadow can surface them without parsing llm_card.
        _trade_candidates_add: list[tuple[str, str]] = [
            ("blocked_reason", "JSONB"),
        ]
        for col, coltype in _trade_candidates_add:
            conn.execute(text(
                f"ALTER TABLE trade_candidates ADD COLUMN IF NOT EXISTS {col} {coltype}"
            ))

        # reconciler_state — key/value store for run counter and future flags.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS reconciler_state (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """))

        # daily_snapshots — begin-of-day P&L snapshot per account (Mon-Fri 09:31 ET).
        # Used to compute daily_pnl = current_total_pnl - snapshot_total_pnl.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_snapshots (
                id             SERIAL PRIMARY KEY,
                snapshot_date  DATE        NOT NULL,
                account_id     TEXT        NOT NULL,
                total_pnl      NUMERIC,
                nav            NUMERIC,
                created_at     TIMESTAMPTZ DEFAULT now(),
                UNIQUE (snapshot_date, account_id)
            )
        """))

        # reconciler_runs — one row per account per reconciler cycle.
        # Used to surface last_synced timestamp per account in GET /accounts.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS reconciler_runs (
                id         SERIAL PRIMARY KEY,
                account_id TEXT        NOT NULL,
                run_at     TIMESTAMPTZ NOT NULL,
                nav        NUMERIC,
                created_at TIMESTAMPTZ DEFAULT now()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_reconciler_runs_account_run_at
            ON reconciler_runs (account_id, run_at DESC)
        """))

        # Unique index on positions.position_key — required for reconciler's
        # ON CONFLICT (position_key) DO NOTHING INSERT.
        # NULL values do not conflict with each other in a UNIQUE index (PostgreSQL
        # semantics), so this is safe even for rows that pre-date position_key.
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_position_key
            ON positions (position_key)
            WHERE position_key IS NOT NULL
        """))

        # One-time migration: add account_id suffix to equity/non-condor position_keys
        # that were created before the cross-account dedup fix.
        # Iron condor keys (colon-namespaced) are left untouched.
        # Idempotent: the NOT LIKE guard skips rows that already have the suffix.
        conn.execute(text("""
            UPDATE positions
            SET position_key = position_key || ':' || account_id
            WHERE strategy IN (
                'EQUITY', 'SHORT_OPTION', 'LONG_OPTION',
                'VERTICAL_SPREAD', 'STRANGLE', 'STRADDLE', 'UNKNOWN'
            )
              AND position_key IS NOT NULL
              AND position_key NOT LIKE '%:' || account_id
              AND account_id IS NOT NULL
              AND account_id != 'PAPER'
        """))

    logger.info("migrate_orders_schema: orders + positions schema verified/migrated")


# ── Goals schema ───────────────────────────────────────────────────────────────

def migrate_goals_schema(engine) -> None:
    """
    Idempotent DDL migration for the sparky_goals table.

    goal_type values : 'weekly' | 'monthly' | 'temporary'
    status values    : 'active' | 'paused' | 'completed' | 'expired' | 'cancelled'
    priority         : integer, lower = higher priority (default 10)
    end_date         : nullable — open-ended goals have no hard deadline

    Safe to call on every startup.  Never drops or renames existing columns.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sparky_goals (
                id          SERIAL PRIMARY KEY,
                goal_type   TEXT        NOT NULL DEFAULT 'monthly',
                goal_text   TEXT        NOT NULL,
                priority    INTEGER     NOT NULL DEFAULT 10,
                start_date  DATE        NOT NULL,
                end_date    DATE,
                status      TEXT        NOT NULL DEFAULT 'active',
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_sparky_goals_status
            ON sparky_goals (status)
        """))
    logger.info("migrate_goals_schema: sparky_goals table verified/migrated")


# ── Daily snapshot ─────────────────────────────────────────────────────────────

def take_daily_snapshot(engine=None) -> None:
    """
    Capture a begin-of-day total_pnl snapshot for every account that has
    open positions with stored unrealized_pnl.

    Inserts one row per account into daily_snapshots for today's date.
    ON CONFLICT DO NOTHING — safe to call multiple times (idempotent).
    Called from an APScheduler cron job at 09:31 ET Mon-Fri in main.py.
    """
    if engine is None:
        engine = create_engine(DB_URL)

    today = datetime.now(timezone.utc).date()
    created_at = datetime.now(timezone.utc)

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT account_id,
                   COALESCE(SUM(unrealized_pnl), 0) AS total_pnl
            FROM positions
            WHERE status = 'open'
              AND unrealized_pnl IS NOT NULL
            GROUP BY account_id
        """)).fetchall()

        inserted = 0
        for r in rows:
            conn.execute(text("""
                INSERT INTO daily_snapshots (snapshot_date, account_id, total_pnl, created_at)
                VALUES (:date, :account_id, :total_pnl, :created_at)
                ON CONFLICT (snapshot_date, account_id) DO NOTHING
            """), {
                "date":       today,
                "account_id": str(r.account_id),
                "total_pnl":  round(float(r.total_pnl), 2),
                "created_at": created_at,
            })
            inserted += 1

    logger.info("take_daily_snapshot: snapshot_date=%s accounts=%d", today, inserted)


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
        from data_layer.provider import get_schwab_client  # noqa: PLC0415
        client = get_schwab_client()
        confirm_live_fill(order_id, client)
        return

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


# ── Live order tracking ─────────────────────────────────────────────────────────

def _resolve_hash_for_account(client, account_id: str) -> str:
    """
    Map a 4-digit account_id to Schwab's accountHash via get_account_numbers().
    Mirrors executor._resolve_account_hash — kept local to avoid circular imports.
    Raises ValueError if not found.
    """
    resp = client.get_account_numbers()
    resp.raise_for_status()
    for entry in (resp.json() or []):
        acct_num = str(entry.get("accountNumber") or "")
        hash_val = str(entry.get("hashValue") or "")
        last4    = acct_num[-4:] if len(acct_num) >= 4 else acct_num
        if last4 == str(account_id) and hash_val:
            return hash_val
    raise ValueError(f"Could not resolve accountHash for account_id={account_id!r}")


def _extract_fill_price(order_data: dict) -> Optional[float]:
    """
    Extract the executed fill price from a Schwab get_order response dict.

    Tries executionLegs[].price first (exact execution price), falls back to
    the order-level 'price' field (limit price / net credit requested).
    Returns None if price cannot be determined.
    """
    try:
        for activity in (order_data.get("orderActivityCollection") or []):
            for leg in (activity.get("executionLegs") or []):
                price = leg.get("price")
                if price is not None:
                    return float(price)
    except Exception:
        pass
    raw = order_data.get("price")
    if raw is not None:
        try:
            return float(raw)
        except (TypeError, ValueError):
            pass
    return None


def get_live_order_status(schwab_order_id, account_hash: str, client) -> str:
    """
    Poll Schwab for the current status of a live order.

    Calls GET /accounts/{accountHash}/orders/{orderId} and maps the Schwab
    status string to our internal status vocabulary:

      FILLED                                         → 'filled'
      REJECTED                                       → 'failed'
      CANCELED / CANCELLED / EXPIRED                 → 'cancelled'
      WORKING + filledQuantity > 0 < orderedQuantity → 'partial_fill'
      anything else (WORKING/QUEUED/AWAITING_*)      → 'submitted' (still in flight)

    Raises httpx.HTTPStatusError on any non-2xx Schwab response.
    """
    resp = client.get_order(int(schwab_order_id), account_hash)
    resp.raise_for_status()
    data = resp.json()

    schwab_status = str(data.get("status", "")).upper()

    if schwab_status == "FILLED":
        return FILLED
    if schwab_status == "REJECTED":
        return FAILED
    if schwab_status in ("CANCELED", "CANCELLED", "EXPIRED"):
        return CANCELLED

    # Partial fill: order still WORKING but some quantity has already executed.
    filled_qty  = float(data.get("filledQuantity", 0) or 0)
    ordered_qty = float(data.get("quantity", 0) or 0)
    if schwab_status == "WORKING" and filled_qty > 0 and filled_qty < ordered_qty:
        return PARTIAL_FILL

    return SUBMITTED  # still in flight: WORKING/QUEUED/AWAITING_* with no fill yet


def confirm_live_fill(order_id: int, client) -> str:
    """
    Check the current Schwab order status and update the orders table.

    Calls get_order via schwab-py, determines internal status, and writes
    the result back to the DB.  Returns the new status string.

    Behavior by resolved status:
      filled       — writes fill_price (from executionLegs or order price),
                     filled_at=now, status='filled'
      partial_fill — logs WARNING, sets status='partial_fill'.
                     Does NOT close the position — the order is still working.
      cancelled    — logs ERROR, sets status='cancelled'
      failed       — logs ERROR, sets status='failed'
      submitted    — order still in flight; no DB change, logs INFO

    Raises:
      ValueError          — order not found or missing schwab_order_id
      httpx.HTTPStatusError — Schwab API returned a non-2xx response
    """
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, account_id, schwab_order_id, quantity, symbol
            FROM orders
            WHERE id = :id
        """), {"id": order_id}).fetchone()

    if not row:
        raise ValueError(f"confirm_live_fill: order id={order_id} not found")

    schwab_order_id = row.schwab_order_id
    account_id      = row.account_id or ""
    symbol          = row.symbol or "?"

    if not schwab_order_id:
        raise ValueError(
            f"confirm_live_fill: order id={order_id} has no schwab_order_id — "
            "cannot poll Schwab for fill status"
        )

    account_hash = _resolve_hash_for_account(client, account_id)

    # Single API call — parse all needed fields from one response.
    resp = client.get_order(int(schwab_order_id), account_hash)
    resp.raise_for_status()
    data = resp.json()

    schwab_status = str(data.get("status", "")).upper()
    filled_qty    = float(data.get("filledQuantity", 0) or 0)
    ordered_qty   = float(data.get("quantity", 0) or 0)

    if schwab_status == "FILLED":
        new_status = FILLED
    elif schwab_status == "REJECTED":
        new_status = FAILED
    elif schwab_status in ("CANCELED", "CANCELLED", "EXPIRED"):
        new_status = CANCELLED
    elif schwab_status == "WORKING" and filled_qty > 0 and filled_qty < ordered_qty:
        new_status = PARTIAL_FILL
    else:
        new_status = SUBMITTED  # still in flight

    now = datetime.now(timezone.utc)

    if new_status == FILLED:
        fill_price = _extract_fill_price(data)
        logger.info(
            "confirm_live_fill: FILLED — order_id=%s schwab_order_id=%s "
            "symbol=%s fill_price=%s",
            order_id, schwab_order_id, symbol, fill_price,
        )
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE orders
                SET status     = 'filled',
                    fill_price = :fill_price,
                    filled_at  = :now
                WHERE id = :id
            """), {"fill_price": fill_price, "now": now, "id": order_id})

    elif new_status == PARTIAL_FILL:
        logger.warning(
            "confirm_live_fill: PARTIAL FILL — order_id=%s schwab_order_id=%s "
            "symbol=%s filledQuantity=%s/%s — position NOT closed, order still working",
            order_id, schwab_order_id, symbol, filled_qty, ordered_qty,
        )
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE orders SET status = 'partial_fill' WHERE id = :id
            """), {"id": order_id})

    elif new_status in (CANCELLED, FAILED):
        logger.error(
            "confirm_live_fill: %s — order_id=%s schwab_order_id=%s "
            "symbol=%s schwab_status=%s",
            new_status.upper(), order_id, schwab_order_id, symbol, schwab_status,
        )
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE orders SET status = :status WHERE id = :id
            """), {"status": new_status, "id": order_id})

    else:  # submitted — still in flight
        logger.info(
            "confirm_live_fill: still in flight — order_id=%s schwab_order_id=%s "
            "schwab_status=%s",
            order_id, schwab_order_id, schwab_status,
        )

    return new_status
