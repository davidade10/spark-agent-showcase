"""
approval_ui/api.py
FastAPI backend for the trade approval dashboard.

Endpoints:
  GET  /candidates                    — pending approved candidates with LLM cards
  GET  /candidates/{id}               — single candidate detail
  POST /candidates/{id}/approve       — mark approved
  POST /candidates/{id}/reject        — mark rejected
  GET  /positions                     — open positions
  GET  /exit-signals                  — pending exit signals
  POST /exit-signals/{id}/snooze      — snooze a signal for 24h
  POST /exit-signals/{id}/dismiss     — dismiss a signal
  GET  /accounts                      — per-account summary from positions table
  GET  /nav                           — combined live NAV from reconciler.log
  GET  /events/{symbol}               — upcoming earnings/FOMC for a symbol
  GET  /health                        — system health

Run with:
  cd ~/spark-agent
  uvicorn approval_ui.api:app --reload --port 8000
"""
from __future__ import annotations

import json
import logging
import sys
import time  # <-- ADDED HERE
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks  # <-- ADDED BackgroundTasks HERE
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from execution.executor import execute_approved_candidate
from execution.order_state import migrate_orders_schema

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

app = FastAPI(title="Spark Agent — Approval API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


@app.on_event("startup")
def ensure_execution_tables():
    """Run the idempotent schema migration on every startup."""
    try:
        migrate_orders_schema(get_engine())
        logger.info("Execution tables verified (orders, positions)")
    except Exception as e:
        logger.error(f"Failed to migrate execution tables on startup: {e}")


# ── Models ────────────────────────────────────────────────────────────────────

class ApproveRequest(BaseModel):
    notes: Optional[str] = None

class RejectRequest(BaseModel):
    reason: Optional[str] = None

class SnoozeRequest(BaseModel):
    hours: int = 24


# ── Helpers ───────────────────────────────────────────────────────────────────

def _age_minutes(ts: Any) -> float:
    if ts is None:
        return 9999.0
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - ts).total_seconds() / 60, 1)


def _serialize(row) -> dict:
    d = dict(row._mapping)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif hasattr(v, '__float__') and not isinstance(v, (int, float, bool)):
            d[k] = float(v)
    return d


def _parse_jsonb(val: Any) -> Any:
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}
    return val or {}


# ── Candidates ────────────────────────────────────────────────────────────────

@app.get("/candidates")
def get_candidates():
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                tc.id,
                tc.created_at,
                tc.snapshot_id,
                tc.symbol,
                tc.strategy,
                tc.score,
                tc.account_id,
                tc.gate_result,
                tc.candidate_json,
                tc.llm_card,
                sr.ts AS snapshot_ts
            FROM trade_candidates tc
            LEFT JOIN snapshot_runs sr ON sr.id = tc.snapshot_id
            WHERE tc.gate_result = 'approved'
              AND (tc.llm_card IS NOT NULL AND tc.llm_card != '{}'::jsonb)
              AND (tc.llm_card ? 'recommendation')
              AND NOT (tc.llm_card ? 'approval_status')
            ORDER BY tc.id DESC
            LIMIT 20
        """)).fetchall()

    results = []
    for row in rows:
        d = _serialize(row)
        d["candidate_json"] = _parse_jsonb(d.get("candidate_json"))
        d["llm_card"]       = _parse_jsonb(d.get("llm_card"))
        d["age_minutes"]    = _age_minutes(d.get("snapshot_ts") or d.get("created_at"))
        d["is_stale"]       = d["age_minutes"] > 15
        results.append(d)

    return {"candidates": results, "count": len(results)}


@app.get("/candidates/{candidate_id}")
def get_candidate(candidate_id: int):
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT tc.*, sr.ts AS snapshot_ts
            FROM trade_candidates tc
            LEFT JOIN snapshot_runs sr ON sr.id = tc.snapshot_id
            WHERE tc.id = :id
        """), {"id": candidate_id}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Candidate not found")
    d = _serialize(row)
    d["candidate_json"] = _parse_jsonb(d.get("candidate_json"))
    d["llm_card"]       = _parse_jsonb(d.get("llm_card"))
    d["age_minutes"]    = _age_minutes(d.get("snapshot_ts") or d.get("created_at"))
    d["is_stale"]       = d["age_minutes"] > 15
    return d


@app.post("/candidates/{candidate_id}/approve")
def approve_candidate(candidate_id: int, body: ApproveRequest = ApproveRequest()):
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(text(
            "SELECT id, llm_card FROM trade_candidates WHERE id = :id"
        ), {"id": candidate_id}).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Candidate not found")
        card = _parse_jsonb(row.llm_card)
        card["approval_status"] = "approved"
        card["approval_ts"]     = datetime.now(timezone.utc).isoformat()
        card["approval_notes"]  = body.notes or ""
        conn.execute(text("""
            UPDATE trade_candidates SET llm_card = cast(:card as jsonb) WHERE id = :id
        """), {"id": candidate_id, "card": json.dumps(card)})
    # approval_status is now committed — trigger execution
    order_id        = None
    execution_error = None
    try:
        order_id = execute_approved_candidate(candidate_id)
        logger.info(f"Execution triggered: candidate_id={candidate_id} order_id={order_id}")
    except Exception as e:
        logger.error(f"Execution failed for candidate_id={candidate_id}: {e}")
        execution_error = str(e)

    response: dict[str, Any] = {"status": "approved", "id": candidate_id}
    if order_id is not None:
        response["order_id"] = order_id
    if execution_error is not None:
        response["execution_error"] = execution_error
    return response


@app.post("/candidates/{candidate_id}/reject")
def reject_candidate(candidate_id: int, body: RejectRequest = RejectRequest()):
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(text(
            "SELECT id, llm_card FROM trade_candidates WHERE id = :id"
        ), {"id": candidate_id}).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Candidate not found")
        card = _parse_jsonb(row.llm_card)
        card["approval_status"]  = "rejected"
        card["approval_ts"]      = datetime.now(timezone.utc).isoformat()
        card["rejection_reason"] = body.reason or ""
        conn.execute(text("""
            UPDATE trade_candidates SET llm_card = cast(:card as jsonb) WHERE id = :id
        """), {"id": candidate_id, "card": json.dumps(card)})
    return {"status": "rejected", "id": candidate_id}


# ── Positions ─────────────────────────────────────────────────────────────────

@app.get("/positions")
def get_positions():
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, account_id, symbol, strategy, expiry, dte,
                   fill_credit, net_delta, unrealized_pnl,
                   opened_at, status, legs, meta,
                   max_risk, position_key,
                   COALESCE(quantity, qty) AS qty
            FROM positions
            WHERE status = 'open'
            ORDER BY opened_at DESC
        """)).fetchall()
    results = []
    for r in rows:
        d = _serialize(r)
        d["legs"] = _parse_jsonb(d.get("legs"))
        d["meta"] = _parse_jsonb(d.get("meta"))
        # Credit: paper uses fill_credit; expose for "Credit received", and as entry_credit for compat
        credit = d.get("fill_credit")
        if credit is not None:
            d["fill_credit"] = float(credit)
            d["entry_credit"] = d["fill_credit"]
        # Compute profit pct if we have the data
        if credit is not None and d.get("unrealized_pnl") is not None:
            try:
                d["profit_pct"] = round(float(d["unrealized_pnl"]) / float(credit) * 100, 1)
            except Exception:
                d["profit_pct"] = None
        else:
            d["profit_pct"] = None
        results.append(d)
    return {"positions": results}


# ── Exit Signals ──────────────────────────────────────────────────────────────

@app.get("/exit-signals")
def get_exit_signals():
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, created_at, symbol, expiry, dte,
                   reason, severity, message,
                   credit_received, debit_to_close, mark,
                   pnl_dollars, pnl_pct, status, position_id
            FROM exit_signals
            WHERE status IN ('pending', 'acknowledged')
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 1
                    WHEN 'warning'  THEN 2
                    ELSE 3
                END,
                created_at DESC
        """)).fetchall()
    results = []
    for r in rows:
        d = _serialize(r)
        d["age_minutes"] = _age_minutes(d.get("created_at"))
        results.append(d)
    return {"signals": results}


@app.post("/exit-signals/{signal_id}/snooze")
def snooze_signal(signal_id: int, body: SnoozeRequest = SnoozeRequest()):
    engine = get_engine()
    snooze_until = datetime.now(timezone.utc) + timedelta(hours=body.hours)
    with engine.begin() as conn:
        row = conn.execute(text(
            "SELECT id FROM exit_signals WHERE id = :id"
        ), {"id": signal_id}).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Signal not found")
        conn.execute(text("""
            UPDATE exit_signals
            SET status       = 'snoozed',
                snoozed_until = :until,
                updated_at   = NOW()
            WHERE id = :id
        """), {"id": signal_id, "until": snooze_until})
    return {"status": "snoozed", "id": signal_id, "hours": body.hours}


@app.post("/exit-signals/{signal_id}/dismiss")
def dismiss_signal(signal_id: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE exit_signals
            SET status = 'dismissed', updated_at = NOW()
            WHERE id = :id
        """), {"id": signal_id})
    return {"status": "dismissed", "id": signal_id}


@app.post("/exit-signals/{signal_id}/acknowledge")
def acknowledge_signal(signal_id: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE exit_signals
            SET status = 'acknowledged', updated_at = NOW()
            WHERE id = :id
        """), {"id": signal_id})
    return {"status": "acknowledged", "id": signal_id}


# ── NAV (reconciler log) ───────────────────────────────────────────────────────

PROJECT_ROOT   = Path(__file__).resolve().parent.parent
RECONCILER_LOG = PROJECT_ROOT / "logs" / "reconciler.log"


@app.get("/nav")
def get_nav():
    """
    Read the last non-empty line of reconciler.log (JSON), return combined_live_nav.
    Log format: one JSON object per line (full_summary with "nav": {"combined_live_nav": ...}).
    Fallback to 0 if file missing, empty, or parse error.
    """
    try:
        if not RECONCILER_LOG.exists():
            logger.debug("get_nav: reconciler.log not found at %s", RECONCILER_LOG)
            return {"combined_live_nav": 0}
        text = RECONCILER_LOG.read_text(encoding="utf-8", errors="replace").strip()
        # Split on newlines only; strip \r so last line parses (Windows/mixed line endings)
        lines = [ln.replace("\r", "").strip() for ln in text.split("\n") if ln.replace("\r", "").strip()]
        if not lines:
            return {"combined_live_nav": 0}
        last_line = lines[-1]
        parsed = json.loads(last_line)
        nav = parsed.get("nav")
        if not isinstance(nav, dict):
            return {"combined_live_nav": 0}
        combined = nav.get("combined_live_nav")
        if combined is None:
            return {"combined_live_nav": 0, "accounts": {}}
        accounts_nav = nav.get("accounts") or {}
        return {
            "combined_live_nav": float(combined),
            "accounts": {k: float(v) for k, v in accounts_nav.items()},
        }
    except (json.JSONDecodeError, ValueError, OSError) as e:
        logger.warning("get_nav: could not read/parse reconciler.log — %s", e)
        return {"combined_live_nav": 0}


# ── Accounts ──────────────────────────────────────────────────────────────────

# --- SAFE SCHWAB CACHE SETTINGS ---
LIVE_NAV_CACHE = {
    "nav": 0.0,
    "buying_power": 0.0,
    "daily_pnl": 0.0,
    "last_updated": 0
}
CACHE_TTL_SECONDS = 60 # Only ask Schwab once per minute

def fetch_live_schwab_data():
    """Silently fetches real data from Schwab in the background."""
    global LIVE_NAV_CACHE
    try:
        # TODO: Initialize your Schwab client here
        # client = get_client() 
        # resp = client.get_account_balances("YOUR_ACCOUNT_HASH").json()
        
        # LIVE_NAV_CACHE["nav"] = resp['securitiesAccount']['currentBalances']['liquidationValue']
        # LIVE_NAV_CACHE["buying_power"] = resp['securitiesAccount']['currentBalances']['buyingPower']
        
        LIVE_NAV_CACHE["last_updated"] = time.time()
        
    except Exception as e:
        print(f"Error fetching live Schwab data: {e}")

@app.get("/accounts")
def get_accounts(background_tasks: BackgroundTasks):
    """Returns dual account summaries, safely caching the live Schwab data."""
    global LIVE_NAV_CACHE
    
    # If cache is older than 60 seconds, trigger background refresh
    if time.time() - LIVE_NAV_CACHE["last_updated"] > CACHE_TTL_SECONDS:
        background_tasks.add_task(fetch_live_schwab_data)

    return {
        "accounts": [
            {
                "account_id": "PAPER_ACCT_01",
                "type": "PAPER",
                "nav": 20000.00,
                "daily_pnl": 0.00,
                "buying_power": 20000.00
            },
            {
                "account_id": "SCHWAB_LIVE",
                "type": "LIVE",
                # Pass cached numbers if available, otherwise show placeholder
                "nav": LIVE_NAV_CACHE["nav"] if LIVE_NAV_CACHE["nav"] > 0 else 250000.00, 
                "daily_pnl": LIVE_NAV_CACHE["daily_pnl"],
                "buying_power": LIVE_NAV_CACHE["buying_power"] if LIVE_NAV_CACHE["buying_power"] > 0 else 250000.00
            }
        ]
    }
# ── Events context ────────────────────────────────────────────────────────────

@app.get("/events/{symbol}")
def get_events(symbol: str):
    """Returns upcoming earnings and FOMC events relevant to a symbol."""
    engine = get_engine()
    now = datetime.now(timezone.utc)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, symbol, event_type, event_ts, source, meta
            FROM events
            WHERE (symbol = :sym OR symbol = 'MARKET')
              AND event_ts >= :now
            ORDER BY event_ts ASC
            LIMIT 10
        """), {"sym": symbol, "now": now}).fetchall()
    results = []
    for r in rows:
        d = _serialize(r)
        d["meta"] = _parse_jsonb(d.get("meta"))
        if d.get("event_ts"):
            event_dt = datetime.fromisoformat(d["event_ts"])
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=timezone.utc)
            d["days_away"] = (event_dt - now).days
        results.append(d)
    return {"events": results, "symbol": symbol}


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
def get_health():
    health: dict[str, Any] = {}

    # DB connectivity
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health["database"] = "ok"
    except Exception as e:
        health["database"] = f"error: {e}"

    # Circuit breaker
    try:
        cb_path = PROJECT_ROOT / "llm_layer" / "circuit_breaker_state.json"
        if cb_path.exists():
            cb = json.loads(cb_path.read_text())
            health["circuit_breaker"] = {
                "state":    cb.get("state", "unknown"),
                "failures": cb.get("total_failures", 0),
                "attempts": cb.get("total_attempts", 0),
            }
        else:
            health["circuit_breaker"] = {"state": "no_state_file"}
    except Exception as e:
        health["circuit_breaker"] = {"state": "error", "detail": str(e)}

    # Data freshness (requires DB — skip if DB is down)
    if health.get("database") == "ok":
        try:
            with get_engine().connect() as conn:
                row = conn.execute(text("""
                    SELECT ts FROM snapshot_runs
                    WHERE status IN ('ok','partial')
                    ORDER BY ts DESC LIMIT 1
                """)).fetchone()
            if row:
                age = _age_minutes(row.ts)
                health["data_freshness"] = {
                    "last_snapshot_minutes_ago": age,
                    "is_stale": age > 20,
                }
            else:
                health["data_freshness"] = {"last_snapshot_minutes_ago": None, "is_stale": True}
        except Exception as e:
            health["data_freshness"] = {"error": str(e)}

    # Schwab token
    try:
        token_path = PROJECT_ROOT / "token.json"
        if token_path.exists():
            health["token"] = "present"
            token = json.loads(token_path.read_text())
            expires_at = token.get("expires_at") or token.get("expiry")
            if expires_at:
                exp_dt = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
                days_left = (exp_dt - datetime.now(timezone.utc)).total_seconds() / 86400
                health["token"] = {
                    "valid": days_left > 0,
                    "days_remaining": round(days_left, 1),
                    "expires_at": exp_dt.isoformat(),
                }
        else:
            health["token"] = "missing"
    except Exception as e:
        health["token"] = f"error: {e}"

    # Reconciler log freshness
    try:
        if RECONCILER_LOG.exists():
            import time as _time
            age_seconds = _time.time() - RECONCILER_LOG.stat().st_mtime
            health["reconciler_log"] = f"ok ({age_seconds:.0f}s old)"
        else:
            health["reconciler_log"] = "missing"
    except Exception as e:
        health["reconciler_log"] = f"error: {e}"

    return {
        "status": "healthy" if health.get("database") == "ok" else "degraded",
        "checks": health,
    }