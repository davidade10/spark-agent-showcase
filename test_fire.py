"""
test_fire.py — One-off diagnostic: verifies the paper execution pipeline end-to-end.

Usage:
    cd ~/spark-agent
    python -m test_fire

Do NOT commit this file.
"""
import json
import sys

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from execution.executor import execute_approved_candidate

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def _migrate_schema(engine) -> None:
    """Add columns that executor.py/dry_run.py expect but are missing from the pre-existing tables."""
    orders_cols = [
        ("candidate_id",  "INTEGER"),
        ("source",        "TEXT DEFAULT 'paper'"),
        ("order_payload", "JSONB"),
        ("fill_price",    "NUMERIC"),
        ("quantity",      "INTEGER"),
        ("created_at",    "TIMESTAMPTZ"),
        ("filled_at",     "TIMESTAMPTZ"),
    ]
    positions_cols = [
        ("long_put_strike",   "NUMERIC"),
        ("short_put_strike",  "NUMERIC"),
        ("short_call_strike", "NUMERIC"),
        ("long_call_strike",  "NUMERIC"),
        ("quantity",          "INTEGER"),
        ("fill_credit",       "NUMERIC"),
        ("order_id",          "INTEGER"),
    ]
    with engine.begin() as conn:
        for col, coltype in orders_cols:
            conn.execute(text(
                f"ALTER TABLE orders ADD COLUMN IF NOT EXISTS {col} {coltype}"
            ))
        for col, coltype in positions_cols:
            conn.execute(text(
                f"ALTER TABLE positions ADD COLUMN IF NOT EXISTS {col} {coltype}"
            ))
        # The old tables have qty/status as NOT NULL without defaults — relax them
        # so executor.py (which writes to the new column names) doesn't fail.
        conn.execute(text("ALTER TABLE orders ALTER COLUMN qty SET DEFAULT 0"))
        conn.execute(text("ALTER TABLE orders ALTER COLUMN qty DROP NOT NULL"))
        conn.execute(text("ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'pending'"))
        conn.execute(text("ALTER TABLE orders ALTER COLUMN account_id SET DEFAULT 'PAPER'"))
        conn.execute(text("ALTER TABLE orders ALTER COLUMN symbol DROP NOT NULL"))
        conn.execute(text("ALTER TABLE positions ALTER COLUMN qty SET DEFAULT 0"))
        conn.execute(text("ALTER TABLE positions ALTER COLUMN qty DROP NOT NULL"))
        conn.execute(text("ALTER TABLE positions ALTER COLUMN account_id SET DEFAULT 'PAPER'"))
        conn.execute(text("ALTER TABLE positions ALTER COLUMN symbol DROP NOT NULL"))
        conn.execute(text("ALTER TABLE positions ALTER COLUMN opened_ts SET DEFAULT now()"))
    print("Schema migration: missing columns added and NOT NULL constraints relaxed.")


def main():
    engine = create_engine(DB_URL)

    # ── 0. Ensure schema is compatible ───────────────────────────────────────
    _migrate_schema(engine)

    # ── 1. Find best IWM APPROVED candidate ──────────────────────────────────
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, candidate_json, llm_card, score
            FROM trade_candidates
            WHERE candidate_json->>'symbol' = 'IWM'
              AND lower(gate_result) = 'approved'
            ORDER BY score DESC
            LIMIT 1
        """)).fetchone()

    if not row:
        print("FAIL — no IWM candidates with gate_result='APPROVED' found in trade_candidates")
        sys.exit(1)

    candidate_id   = row.id
    candidate_json = row.candidate_json if isinstance(row.candidate_json, dict) else json.loads(row.candidate_json or "{}")
    llm_card       = row.llm_card       if isinstance(row.llm_card,       dict) else json.loads(row.llm_card       or "{}")
    score          = row.score

    expiry     = candidate_json.get("expiry",     "n/a")
    net_credit = candidate_json.get("net_credit", "n/a")

    print("─" * 60)
    print(f"Candidate found:")
    print(f"  id          : {candidate_id}")
    print(f"  expiry      : {expiry}")
    print(f"  score       : {score}")
    print(f"  net_credit  : {net_credit}")
    print(f"  approval_status (before): {llm_card.get('approval_status', '<not set>')}")
    print("─" * 60)

    # ── 2. Simulate dashboard approve click if not already approved ───────────
    if llm_card.get("approval_status") != "approved":
        print("approval_status is not 'approved' — patching llm_card (simulating dashboard click)...")
        from datetime import datetime, timezone
        llm_card["approval_status"] = "approved"
        llm_card["approval_ts"]     = datetime.now(timezone.utc).isoformat()
        llm_card["approval_notes"]  = "set by test_fire.py"
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE trade_candidates
                SET llm_card = cast(:card as jsonb)
                WHERE id = :id
            """), {"id": candidate_id, "card": json.dumps(llm_card)})
        print(f"  llm_card patched — approval_status='approved'")
    else:
        print("approval_status already 'approved' — no patch needed")

    print("─" * 60)

    # ── 3. Execute ────────────────────────────────────────────────────────────
    print(f"Calling execute_approved_candidate({candidate_id})...")
    try:
        order_id = execute_approved_candidate(candidate_id)
    except Exception as e:
        print(f"FAIL — execute_approved_candidate raised: {e}")
        sys.exit(1)

    print(f"  order_id returned: {order_id}")
    print("─" * 60)

    # ── 4. Query orders row ───────────────────────────────────────────────────
    with engine.connect() as conn:
        order_row = conn.execute(text("""
            SELECT id, candidate_id, symbol, status, fill_price, quantity, source
            FROM orders
            WHERE id = :id
        """), {"id": order_id}).fetchone()

    if not order_row:
        print(f"FAIL — no row found in orders for id={order_id}")
        sys.exit(1)

    print("orders row:")
    print(f"  id           : {order_row.id}")
    print(f"  candidate_id : {order_row.candidate_id}")
    print(f"  symbol       : {order_row.symbol}")
    print(f"  status       : {order_row.status}")
    print(f"  fill_price   : {order_row.fill_price}")
    print(f"  quantity     : {order_row.quantity}")
    print(f"  source       : {order_row.source}")
    print("─" * 60)

    # ── 5. Query positions row ────────────────────────────────────────────────
    with engine.connect() as conn:
        pos_row = conn.execute(text("""
            SELECT id, symbol, expiry, short_put_strike, short_call_strike,
                   quantity, fill_credit, status, account_id
            FROM positions
            WHERE order_id = :order_id
        """), {"order_id": order_id}).fetchone()

    if not pos_row:
        print(f"FAIL — no row found in positions for order_id={order_id}")
        sys.exit(1)

    print("positions row:")
    print(f"  id                : {pos_row.id}")
    print(f"  symbol            : {pos_row.symbol}")
    print(f"  expiry            : {pos_row.expiry}")
    print(f"  short_put_strike  : {pos_row.short_put_strike}")
    print(f"  short_call_strike : {pos_row.short_call_strike}")
    print(f"  quantity          : {pos_row.quantity}")
    print(f"  fill_credit       : {pos_row.fill_credit}")
    print(f"  status            : {pos_row.status}")
    print(f"  account_id        : {pos_row.account_id}")
    print("─" * 60)

    # ── 6. PASS / FAIL ────────────────────────────────────────────────────────
    failures = []

    if order_row.status != "filled":
        failures.append(f"orders.status='{order_row.status}' (expected 'filled')")

    if pos_row.account_id != "PAPER":
        failures.append(f"positions.account_id='{pos_row.account_id}' (expected 'PAPER')")

    if pos_row.status != "open":
        failures.append(f"positions.status='{pos_row.status}' (expected 'open')")

    if failures:
        print("RESULT: FAIL")
        for f in failures:
            print(f"  ✗ {f}")
        sys.exit(1)
    else:
        print("RESULT: PASS")
        print(f"  ✓ orders.status='filled'")
        print(f"  ✓ positions.account_id='PAPER'")
        print(f"  ✓ positions.status='open'")


if __name__ == "__main__":
    main()
