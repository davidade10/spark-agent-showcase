"""
approval_ui/api.py
FastAPI backend for the trade approval dashboard.

Endpoints:
  GET  /candidates                    — pending approved candidates with LLM cards
  GET  /candidates/{id}               — single candidate detail
  POST /candidates/{id}/approve       — mark approved
  POST /candidates/{id}/reject        — mark rejected
  POST /candidates/{id}/delegate      — notify Telegram (Ask Sparky)
  GET  /positions                     — open positions
  POST /positions/{id}/close          — close a PAPER position (paper_close)
  GET  /exit-signals                  — pending exit signals
  POST /exit-signals/{id}/snooze      — snooze a signal for 24h
  POST /exit-signals/{id}/dismiss     — dismiss a signal
  POST /refresh                       — trigger collection/pricing refresh; returns structured result
  GET  /accounts                      — per-account summary from positions table
  GET  /nav                           — combined live NAV from reconciler.log
  GET  /events/{symbol}               — upcoming earnings/FOMC for a symbol
  GET  /health                        — system health
  GET  /shadow?hours=N                — blocked candidates (gate_result='blocked'), last N hours (default 48, max 168)
  GET  /history?days=N               — decision history with P&L (default 30 days, max 90)
  GET  /pipeline-stats?hours=N       — funnel counts over trade_candidates window (default 48h, max 168h)

Run with:
  cd ~/spark-agent
  uvicorn approval_ui.api:app --reload --port 8000
"""
from __future__ import annotations

import json
import logging
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    PAPER_ACCOUNT_STARTING_BALANCE,
    APPROVAL_STALENESS_LIMIT_SECONDS,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
)
from data_layer.collector import run_collection_cycle
from data_layer.notifier import send_telegram_msg
from data_layer.freshness import check_data_freshness, is_market_open
from data_layer.provider import get_schwab_client, AuthenticationRequiredError
from execution.close_paper_position import close_paper_position
from execution.executor import execute_approved_candidate
from execution.order_state import migrate_orders_schema, migrate_goals_schema
from strategy_engine.exit_monitor import run_exit_scan
from strategy_engine.watchlist_screener import run_screener

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Single shared SQLAlchemy engine for the API process. Endpoints should
# continue to acquire per-request connections via engine.connect()/begin().
ENGINE = create_engine(DB_URL, pool_pre_ping=True)


app = FastAPI(title="Spark Agent — Approval API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)
def get_engine():
    return ENGINE


def migrate_agent_config(engine) -> None:
    """
    Idempotent migration for the agent_config table.
    Creates table and seeds default values if they don't already exist.
    Safe to call on every startup.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS agent_config (
                key         TEXT PRIMARY KEY,
                value       TEXT NOT NULL,
                description TEXT,
                updated_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            INSERT INTO agent_config (key, value, description) VALUES
              ('autonomous_trading_enabled',    'false', 'Master switch for autonomous paper trading. Set to true to enable Stage 3.'),
              ('autonomous_score_threshold',    '80',    'Minimum score for autonomous approval'),
              ('autonomous_confidence_threshold','0.75', 'Minimum LLM confidence for autonomous approval'),
              ('dte_alert_critical',            '7',     'DTE threshold for critical close alert'),
              ('dte_alert_manage',              '21',    'DTE threshold for management window'),
              ('profit_target_pct',             '50',    'Profit capture % to recommend close'),
              ('strangle_trading_enabled',      'false', 'Master switch for STRANGLE strategy. Set to true to enable strangle trading.')
            ON CONFLICT (key) DO NOTHING
        """))


@app.on_event("startup")
def ensure_execution_tables():
    """Run the idempotent schema migration on every startup."""
    try:
        migrate_orders_schema(get_engine())
        logger.info("Execution tables verified (orders, positions)")
    except Exception as e:
        logger.error(f"Failed to migrate execution tables on startup: {e}")
    try:
        migrate_agent_config(get_engine())
        logger.info("agent_config table verified")
    except Exception as e:
        logger.error(f"Failed to migrate agent_config on startup: {e}")
    try:
        migrate_goals_schema(get_engine())
        logger.info("sparky_goals table verified")
    except Exception as e:
        logger.error(f"Failed to migrate sparky_goals on startup: {e}")


# ── Models ────────────────────────────────────────────────────────────────────

class ApproveRequest(BaseModel):
    notes: Optional[str] = None

class RejectRequest(BaseModel):
    reason: Optional[str] = None

class SnoozeRequest(BaseModel):
    hours: int = 24


class ClosePositionRequest(BaseModel):
    exit_debit: float
    exit_reason: str = "MANUAL_CLOSE"


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


def _iron_condor_spread_width_from_strikes(d: dict) -> Optional[float]:
    """Average put-side and call-side width when all four condor strikes are present."""
    try:
        lp = float(d["long_put_strike"])
        sp = float(d["short_put_strike"])
        scc = float(d["short_call_strike"])
        lc = float(d["long_call_strike"])
        w_put = sp - lp
        w_call = lc - scc
        if w_put > 0 and w_call > 0:
            return round((w_put + w_call) / 2.0, 6)
    except (TypeError, ValueError, KeyError):
        pass
    return None


# Sentinel for blocked rows with missing / legacy blocked_reason.rule (heatmap bucketing)
_UNKNOWN_LEGACY_RULE = "__UNKNOWN_LEGACY__"


def _safe_json_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def _safe_qty_from_candidate_json(cj: dict) -> Optional[int]:
    for k in ("qty", "contracts", "quantity"):
        if k not in cj:
            continue
        x = _safe_json_float(cj.get(k))
        if x is not None and x >= 0:
            return int(round(x))
    return None


def _flatten_condor_spread_fields(
    candidate_json: Any,
    strategy_from_row: Optional[str],
) -> dict[str, Any]:
    """Extract iron-condor legs + qty from candidate_json (additive API fields)."""
    cj = candidate_json if isinstance(candidate_json, dict) else {}
    strat = strategy_from_row or cj.get("strategy")
    return {
        "strategy": strat,
        "long_put_strike": _safe_json_float(cj.get("long_put_strike")),
        "short_put_strike": _safe_json_float(cj.get("short_put_strike")),
        "short_call_strike": _safe_json_float(cj.get("short_call_strike")),
        "long_call_strike": _safe_json_float(cj.get("long_call_strike")),
        "qty": _safe_qty_from_candidate_json(cj),
    }


def _blocked_rule_bucket_key(br: Any) -> str:
    """Stable key for gate-kill distribution + labels (legacy → sentinel)."""
    if not isinstance(br, dict):
        return _UNKNOWN_LEGACY_RULE
    r = br.get("rule")
    if r is not None and str(r).strip():
        return str(r).strip()
    return _UNKNOWN_LEGACY_RULE


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
              AND COALESCE(tc.llm_card->>'approval_status', '') NOT IN ('approved', 'working', 'stale', 'expired')
            ORDER BY tc.id DESC
            LIMIT 20
        """)).fetchall()

    stale_limit_minutes = APPROVAL_STALENESS_LIMIT_SECONDS / 60
    results = []
    for row in rows:
        d = _serialize(row)
        d["candidate_json"] = _parse_jsonb(d.get("candidate_json"))
        d["llm_card"]       = _parse_jsonb(d.get("llm_card"))
        d["age_minutes"]    = _age_minutes(d.get("snapshot_ts") or d.get("created_at"))
        d["is_stale"]       = d["age_minutes"] > stale_limit_minutes
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
    d["is_stale"]       = d["age_minutes"] > (APPROVAL_STALENESS_LIMIT_SECONDS / 60)
    return d


@app.post("/candidates/{candidate_id}/approve")
def approve_candidate(candidate_id: int, body: ApproveRequest = ApproveRequest()):
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT tc.id, tc.llm_card, tc.created_at,
                   sr.ts AS snapshot_ts
            FROM trade_candidates tc
            LEFT JOIN snapshot_runs sr ON sr.id = tc.snapshot_id
            WHERE tc.id = :id
        """), {"id": candidate_id}).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Candidate not found")

        # Idempotency / safety: prevent re-approving candidates that are already
        # in-flight or fully processed. Allow retries for transient failures
        # (error/rejected) so the UI can resubmit after fixing issues.
        card_existing = _parse_jsonb(row.llm_card)
        existing_status = (card_existing or {}).get("approval_status")
        if existing_status in {"approved", "working"}:
            raise HTTPException(
                status_code=400,
                detail=f"Candidate already {existing_status or 'processed'}; cannot approve again.",
            )

        # ── Freshness gate ────────────────────────────────────────────────────
        # Priority 1: snapshot_ts — the exact snapshot_run this candidate was
        #   built from (strongest guarantee of data recency).
        # Priority 2: candidate's own created_at — used when snapshot_id is NULL.
        # Fail closed if neither is resolvable.
        freshness_ts = row.snapshot_ts or row.created_at
        if freshness_ts is None:
            raise HTTPException(
                status_code=422,
                detail="Cannot verify candidate freshness — no snapshot timestamp found. Refusing to approve.",
            )
        if freshness_ts.tzinfo is None:
            freshness_ts = freshness_ts.replace(tzinfo=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - freshness_ts).total_seconds()
        if age_seconds > APPROVAL_STALENESS_LIMIT_SECONDS:
            age_min = int(age_seconds / 60)
            raise HTTPException(
                status_code=422,
                detail=f"Candidate data is stale ({age_min} min old). Refresh data and regenerate before approving.",
            )
        # ── End freshness gate ────────────────────────────────────────────────

        card = card_existing or {}
        card["approval_status"] = "approved"
        card["approval_ts"]     = datetime.now(timezone.utc).isoformat()
        card["approval_notes"]  = body.notes or ""
        conn.execute(text("""
            UPDATE trade_candidates SET llm_card = cast(:card as jsonb) WHERE id = :id
        """), {"id": candidate_id, "card": json.dumps(card)})
    # approval_status is now committed — trigger execution via executor
    order_id        = None
    execution_error = None
    try:
        order_id = execute_approved_candidate(candidate_id)
        logger.info(f"Execution triggered: candidate_id={candidate_id} order_id={order_id}")
    except Exception as e:
        logger.error(f"Execution failed for candidate_id={candidate_id}: {e}")
        execution_error = str(e)

    # Persist post-execution state back into llm_card so the UI can reflect it.
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(text(
            "SELECT llm_card FROM trade_candidates WHERE id = :id"
        ), {"id": candidate_id}).fetchone()
        card = _parse_jsonb(row.llm_card) if row else {}

        if order_id is not None and execution_error is None:
            card["approval_status"] = "working"
            card["order_id"]        = order_id
        else:
            card["approval_status"]  = "error"
            card["execution_error"]  = execution_error or "Unknown execution error"
            card["approval_error_ts"] = datetime.now(timezone.utc).isoformat()

        conn.execute(text("""
            UPDATE trade_candidates SET llm_card = cast(:card as jsonb) WHERE id = :id
        """), {"id": candidate_id, "card": json.dumps(card)})

        try:
            conn.execute(text("""
                INSERT INTO trade_decisions (candidate_id, decision, decided_at)
                VALUES (:cid, 'approved', NOW())
            """), {"cid": candidate_id})
        except Exception as td_exc:
            logger.error(f"trade_decisions INSERT failed for candidate_id={candidate_id}: {td_exc}")

    response: dict[str, Any] = {
        "status":   "working" if order_id is not None and execution_error is None else "error",
        "id":       candidate_id,
        "order_id": order_id,
    }
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

        td_existing = conn.execute(text("""
            SELECT decision
            FROM trade_decisions
            WHERE candidate_id = :cid
            ORDER BY id DESC
            LIMIT 1
        """), {"cid": candidate_id}).fetchone()

        if td_existing is not None:
            prior = (str(td_existing.decision or "")).strip().lower()
            if prior == "rejected":
                # Idempotent double-submit — safe no-op for trade_decisions
                return {"status": "rejected", "id": candidate_id}
            if prior in ("approved", "working"):
                raise HTTPException(
                    status_code=409,
                    detail="Candidate already approved — cannot reject",
                )
            raise HTTPException(
                status_code=409,
                detail="Candidate already has a decision — cannot reject",
            )

        card = _parse_jsonb(row.llm_card)
        card["approval_status"]  = "rejected"
        card["approval_ts"]      = datetime.now(timezone.utc).isoformat()
        card["rejection_reason"] = body.reason or ""
        conn.execute(text("""
            UPDATE trade_candidates SET llm_card = cast(:card as jsonb) WHERE id = :id
        """), {"id": candidate_id, "card": json.dumps(card)})

        conn.execute(text("""
            INSERT INTO trade_decisions (candidate_id, decision, reason, decided_at)
            VALUES (:cid, 'rejected', :reason, NOW())
        """), {"cid": candidate_id, "reason": body.reason or None})

    return {"status": "rejected", "id": candidate_id}


@app.post("/candidates/{candidate_id}/delegate")
def delegate_candidate(candidate_id: int):
    """
    Notify configured Telegram chat that a candidate needs human review (Ask Sparky).
    Read-only on trade_candidates; does not change approval state.
    """
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT tc.symbol, tc.strategy, tc.score
                FROM trade_candidates tc
                WHERE tc.id = :id
            """),
            {"id": candidate_id},
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Candidate not found")

    d = _serialize(row)
    symbol = (d.get("symbol") or "UNKNOWN").strip() or "UNKNOWN"
    strategy_raw = d.get("strategy")
    strategy = (str(strategy_raw).strip() if strategy_raw is not None else "—") or "—"
    strategy_display = strategy.replace("_", " ")
    score_val = d.get("score")
    if score_val is not None:
        try:
            score_display = f"{float(score_val):.0f}"
        except (TypeError, ValueError):
            score_display = "—"
    else:
        score_display = "—"

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": "Telegram not configured"},
        )

    # Telegram Markdown: avoid raw underscores in strategy (e.g. IRON_CONDOR) breaking parse.
    text_msg = (
        f"Candidate {candidate_id} pending review — {symbol} {strategy_display} score {score_display}.\n"
        f"Please advise approve or reject."
    )
    if not send_telegram_msg(text_msg):
        return JSONResponse(
            status_code=502,
            content={"success": False, "error": "Telegram send failed"},
        )
    return {"success": True}


# ── Positions ─────────────────────────────────────────────────────────────────

@app.get("/positions")
def get_positions(include_hidden: bool = Query(False, description="Include imbalanced/UNKNOWN rows for debug view")):
    """
    Returns open positions. When include_hidden=True, also returns status='imbalanced'
    and strategy='UNKNOWN' rows for the Debug / Hidden Rows view.
    """
    engine = get_engine()
    if include_hidden:
        where = "status IN ('open', 'imbalanced')"
    else:
        where = "status = 'open'"
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, account_id, symbol, strategy, expiry, dte,
                   fill_credit, net_delta, unrealized_pnl, mark,
                   opened_at, status, legs, legs_json, meta,
                   long_put_strike, short_put_strike, short_call_strike, long_call_strike,
                   max_risk, position_key,
                   COALESCE(quantity, qty) AS qty
            FROM positions
            WHERE {where}
            ORDER BY opened_at DESC
        """)).fetchall()
    results = []
    for r in rows:
        d = _serialize(r)
        d["legs"] = _parse_jsonb(d.get("legs"))
        d["legs_json"] = _parse_jsonb(d.get("legs_json"))
        d["meta"] = _parse_jsonb(d.get("meta"))
        # Credit: paper uses fill_credit; expose for "Credit received", and as entry_credit for compat
        credit = d.get("fill_credit")
        if credit is not None:
            d["fill_credit"] = float(credit)
            d["entry_credit"] = d["fill_credit"]
            d["net_credit"] = d["fill_credit"]
        meta = d.get("meta") if isinstance(d.get("meta"), dict) else {}
        sw_raw = meta.get("spread_width") if isinstance(meta, dict) else None
        spread_width: Optional[float] = None
        if sw_raw is not None:
            try:
                spread_width = float(sw_raw)
            except (TypeError, ValueError):
                spread_width = None
        if spread_width is None and (d.get("strategy") or "").upper() == "IRON_CONDOR":
            spread_width = _iron_condor_spread_width_from_strikes(d)
        d["spread_width"] = spread_width
        # Compute profit pct for credit spreads:
        # unrealized_pnl is stored in dollars; fill_credit is per-contract premium.
        # Use total entry credit dollars as the denominator when possible.
        qty = d.get("qty")
        unrealized = d.get("unrealized_pnl")
        profit_pct = None
        try:
            if credit is not None and unrealized is not None and qty is not None:
                total_entry = float(credit) * float(qty) * 100.0
                if total_entry > 0:
                    profit_pct = float(unrealized) / total_entry * 100.0
        except Exception:
            profit_pct = None
        d["profit_pct"] = None if profit_pct is None else round(profit_pct, 1)
        results.append(d)
    return {"positions": results}


@app.post("/positions/{position_id}/close")
def post_close_position(position_id: int, body: ClosePositionRequest):
    """
    Close a paper position using execution.close_paper_position.
    Live accounts are rejected until a live close path exists.
    """
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, account_id, status FROM positions WHERE id = :pid"
            ),
            {"pid": position_id},
        ).fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Position {position_id} not found",
        )

    account_id = str(row.account_id or "")
    status = str(row.status or "")

    if account_id != "PAPER":
        raise HTTPException(
            status_code=422,
            detail="Live position close not yet supported",
        )

    if status != "open":
        raise HTTPException(
            status_code=422,
            detail=f"Position {position_id} is not open (status={status!r})",
        )

    try:
        result = close_paper_position(
            position_id,
            body.exit_debit,
            body.exit_reason,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception as e:
        logger.exception("post_close_position failed for position_id=%s", position_id)
        raise HTTPException(
            status_code=500,
            detail=str(e) or "Close failed",
        ) from e

    return {
        "success": True,
        "pnl": result["pnl"],
        "trade_outcome_id": result["trade_outcome_id"],
    }


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

# Used for execution position sizing only — NOT for NAV display.
# NAV display uses get_paper_nav() which computes starting balance + realized P&L.
PAPER_ACCOUNT_NAV: float = float(
    __import__("os").getenv("PAPER_ACCOUNT_NAV", "20000")
)


def get_paper_nav(conn) -> float:
    """
    Compute PAPER account NAV as realized P&L layered on top of the
    historical starting balance:

        paper_nav = PAPER_ACCOUNT_STARTING_BALANCE
                    + SUM(trade_outcomes.pnl WHERE account_id = 'PAPER')

    trade_outcomes is a closed-outcomes-only table — every row is a
    finalized realized P&L written by close_paper_position().  No
    additional open/closed filter is needed.

    Returns PAPER_ACCOUNT_STARTING_BALANCE when no PAPER outcomes exist yet.
    """
    row = conn.execute(text("""
        SELECT COALESCE(SUM(pnl), 0) AS total_pnl
        FROM trade_outcomes
        WHERE account_id = 'PAPER'
    """)).fetchone()
    total_pnl = float(row.total_pnl) if row else 0.0
    return round(PAPER_ACCOUNT_STARTING_BALANCE + total_pnl, 2)


def _read_reconciler_nav() -> dict[str, Any]:
    """
    Parse the last non-empty line of reconciler.log and return the nav dict.
    Returns {"accounts": {}, "combined_live_nav": 0} on any failure.
    """
    empty: dict[str, Any] = {"accounts": {}, "combined_live_nav": 0}
    try:
        if not RECONCILER_LOG.exists():
            return empty
        raw = RECONCILER_LOG.read_text(encoding="utf-8", errors="replace").strip()
        lines = [ln.replace("\r", "").strip() for ln in raw.split("\n") if ln.replace("\r", "").strip()]
        if not lines:
            return empty
        parsed = json.loads(lines[-1])
        nav = parsed.get("nav")
        if not isinstance(nav, dict):
            return empty
        return {
            "accounts":         {k: float(v) for k, v in (nav.get("accounts") or {}).items()},
            "combined_live_nav": float(nav.get("combined_live_nav") or 0),
        }
    except Exception as e:
        logger.warning("_read_reconciler_nav: could not parse reconciler.log — %s", e)
        return empty


def _fetch_schwab_account_details(client, account_hash: str) -> dict:
    """
    Fetch buying_power and total_margin from Schwab's currentBalances for a live account.

    daily_pnl is always None — Schwab does not expose a per-day P&L value in
    currentBalances; computing it would require a begin-of-day NAV snapshot.

    Uses a 3-second ThreadPoolExecutor timeout so a slow or unavailable Schwab
    call never blocks the /accounts response. Returns all-None on any failure.
    Never raises.
    """
    import concurrent.futures
    null_result: dict[str, Any] = {"buying_power": None, "total_margin": None, "daily_pnl": None}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(client.get_account, account_hash)
            try:
                resp = future.result(timeout=3)
            except concurrent.futures.TimeoutError:
                logger.warning(
                    "_fetch_schwab_account_details: timed out (3s) for hash %s…",
                    account_hash[:8],
                )
                return null_result
        resp.raise_for_status()
        balances = (
            resp.json()
                .get("securitiesAccount", {})
                .get("currentBalances", {})
            or {}
        )
        buying_power = balances.get("buyingPower") or balances.get("availableFunds")
        total_margin = balances.get("maintenanceRequirement")
        return {
            "buying_power": round(float(buying_power), 2) if buying_power is not None else None,
            "total_margin": round(float(total_margin), 2) if total_margin is not None else None,
            "daily_pnl":    None,
        }
    except Exception as exc:
        logger.warning("_fetch_schwab_account_details: failed — %s", exc)
        return null_result


@app.get("/accounts")
def get_accounts():
    """
    Per-account summary built from:
      - reconciler.log  → per-account NAV (accounts dict keyed by last-4 digits)
      - positions table → open position counts, total_credit, total_pnl (all accounts)
      - Schwab /accounts API → buying_power, total_margin for live accounts (best-effort)

    daily_pnl: always null — no begin-of-day snapshot exists.
    buying_power (PAPER): computed as nav - total_max_risk from strike columns;
                          null if any open option position is missing strike data.
    total_margin: always null for PAPER (margin does not apply).
    """
    import concurrent.futures

    nav_data     = _read_reconciler_nav()
    accounts_nav = nav_data["accounts"]   # {"8096": 8066.71, "5760": 6596.56}

    # ── DB stats ──────────────────────────────────────────────────────────────
    pos_counts:    dict[str, int]        = {}
    total_credits: dict[str, float]      = {}
    total_pnls:    dict[str, float]      = {}
    daily_pnls:    dict[str, float|None] = {}
    last_synced:   dict[str, str|None]   = {}
    paper_nav:     float                 = PAPER_ACCOUNT_STARTING_BALANCE
    paper_bp:      float | None          = None

    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Position counts per account
            for r in conn.execute(text("""
                SELECT account_id, COUNT(*) AS cnt
                FROM positions WHERE status = 'open'
                GROUP BY account_id
            """)).fetchall():
                pos_counts[str(r.account_id)] = int(r.cnt)

            paper_nav = get_paper_nav(conn)

            # Total credit collected — options only (IC + SO).
            # COALESCE(quantity, qty) handles legacy rows that predate the Phase 5 column rename.
            for r in conn.execute(text("""
                SELECT account_id,
                       COALESCE(SUM(fill_credit * COALESCE(quantity, qty) * 100), 0) AS total_credit
                FROM positions
                WHERE status = 'open'
                  AND strategy IN ('IRON_CONDOR', 'SHORT_OPTION')
                GROUP BY account_id
            """)).fetchall():
                total_credits[str(r.account_id)] = round(float(r.total_credit), 2)

            # Unrealized P&L — options only, mark must be present
            for r in conn.execute(text("""
                SELECT account_id,
                       COALESCE(SUM(
                           (fill_credit - mark) * COALESCE(quantity, qty) * 100
                       ), 0) AS total_pnl
                FROM positions
                WHERE status = 'open'
                  AND strategy IN ('IRON_CONDOR', 'SHORT_OPTION')
                  AND mark IS NOT NULL
                GROUP BY account_id
            """)).fetchall():
                total_pnls[str(r.account_id)] = round(float(r.total_pnl), 2)

            # Equity unrealized P&L — join with underlying_quotes for current price.
            # Positions missing a price are excluded and logged as warnings.
            eq_rows = conn.execute(text("""
                SELECT id, account_id, symbol, fill_credit,
                       COALESCE(quantity, qty) AS qty
                FROM positions
                WHERE status = 'open'
                  AND strategy = 'EQUITY'
            """)).fetchall()
            _equity_missing: list[str] = []
            for er in eq_rows:
                acct = str(er.account_id)
                price_row = conn.execute(text("""
                    SELECT price FROM underlying_quotes
                    WHERE symbol = :sym
                    ORDER BY ts DESC
                    LIMIT 1
                """), {"sym": er.symbol}).fetchone()
                if price_row is None:
                    _equity_missing.append(er.symbol)
                    continue
                eq_pnl = (float(price_row.price) - float(er.fill_credit)) * int(er.qty)
                total_pnls[acct] = round(total_pnls.get(acct, 0.0) + eq_pnl, 2)
            if _equity_missing:
                logger.warning(
                    "get_accounts: equity positions missing price in underlying_quotes — "
                    "excluded from total_pnl: %s", ", ".join(_equity_missing)
                )

            # Daily P&L — current total_pnl minus today's 09:31 snapshot.
            try:
                today = datetime.now(timezone.utc).date()
                for r in conn.execute(text("""
                    SELECT account_id, total_pnl
                    FROM daily_snapshots
                    WHERE snapshot_date = :today
                """), {"today": today}).fetchall():
                    daily_pnls[str(r.account_id)] = float(r.total_pnl)
            except Exception as _e:
                logger.warning("get_accounts: daily_snapshots query failed — %s", _e)

            # Last reconciler run per live account.
            try:
                for r in conn.execute(text("""
                    SELECT account_id, MAX(run_at) AS last_synced
                    FROM reconciler_runs
                    GROUP BY account_id
                """)).fetchall():
                    ts = r.last_synced
                    last_synced[str(r.account_id)] = ts.isoformat() if ts else None
            except Exception as _e:
                logger.warning("get_accounts: reconciler_runs query failed — %s", _e)

            # PAPER buying_power = nav - total_max_risk_at_stake.
            # Requires all four strike columns populated on every open option position.
            # If any position is missing strike data, set buying_power to null.
            missing_strikes = conn.execute(text("""
                SELECT COUNT(*) FROM positions
                WHERE account_id = 'PAPER'
                  AND status = 'open'
                  AND strategy IN ('IRON_CONDOR', 'SHORT_OPTION')
                  AND (
                    long_put_strike   IS NULL OR short_put_strike  IS NULL
                    OR short_call_strike IS NULL OR long_call_strike IS NULL
                  )
            """)).scalar() or 0

            if missing_strikes == 0:
                mr_row = conn.execute(text("""
                    SELECT COALESCE(SUM(
                        (GREATEST(
                            short_call_strike - long_call_strike,
                            short_put_strike  - long_put_strike
                        ) - fill_credit) * COALESCE(quantity, qty) * 100
                    ), 0) AS total_max_risk
                    FROM positions
                    WHERE account_id = 'PAPER'
                      AND status = 'open'
                      AND strategy IN ('IRON_CONDOR', 'SHORT_OPTION')
                """)).fetchone()
                if mr_row is not None:
                    paper_bp = round(paper_nav - float(mr_row.total_max_risk), 2)
            else:
                logger.warning(
                    "get_accounts: %d PAPER position(s) missing strike data — buying_power null",
                    missing_strikes,
                )

    except Exception as e:
        logger.warning("get_accounts: DB query failed — %s", e)

    # ── Schwab details for live accounts (best-effort, 3s timeout each) ───────
    schwab_details: dict[str, dict] = {}
    try:
        sc = get_schwab_client(interactive=False)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(sc.get_account_numbers)
            try:
                acct_resp = future.result(timeout=3)
            except concurrent.futures.TimeoutError:
                logger.warning("get_accounts: get_account_numbers() timed out — live details null")
                acct_resp = None
        if acct_resp is not None:
            acct_resp.raise_for_status()
            for entry in acct_resp.json() or []:
                acct_num = str(entry.get("accountNumber") or "")
                hash_val = str(entry.get("hashValue") or "")
                last4    = acct_num[-4:] if len(acct_num) >= 4 else acct_num
                if last4 and hash_val:
                    schwab_details[last4] = _fetch_schwab_account_details(sc, hash_val)
    except Exception as exc:
        logger.debug("get_accounts: Schwab details unavailable — %s", exc)

    # ── Build result ──────────────────────────────────────────────────────────
    result: list[dict[str, Any]] = []

    def _daily_pnl(acct: str, cur_pnl: float | None) -> float | None:
        snap = daily_pnls.get(acct)
        if snap is None or cur_pnl is None:
            return None
        return round(cur_pnl - snap, 2)

    for last4, nav_val in accounts_nav.items():
        sd = schwab_details.get(last4, {})
        cur = total_pnls.get(last4)
        result.append({
            "account_id":     last4,
            "type":           "LIVE",
            "nav":            nav_val,
            "open_positions": pos_counts.get(last4, 0),
            "buying_power":   sd.get("buying_power"),
            "daily_pnl":      _daily_pnl(last4, cur),
            "total_credit":   total_credits.get(last4),
            "total_margin":   sd.get("total_margin"),
            "total_pnl":      cur,
            "last_synced":    last_synced.get(last4),
        })

    paper_cur = total_pnls.get("PAPER")
    result.append({
        "account_id":     "PAPER",
        "type":           "PAPER",
        "nav":            paper_nav,
        "open_positions": pos_counts.get("PAPER", 0),
        "buying_power":   paper_bp,
        "daily_pnl":      _daily_pnl("PAPER", paper_cur),
        "total_credit":   total_credits.get("PAPER"),
        "total_margin":   None,
        "total_pnl":      paper_cur,
        "last_synced":    None,
    })

    return {"accounts": result}
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


# ── Refresh ────────────────────────────────────────────────────────────────────

@app.post("/refresh")
def post_refresh():
    """
    Trigger a refresh cycle: try collection when market is open, then run
    pricing-only from the latest snapshot. Always returns a structured result
    so the UI can show clear feedback (success / partial / failure).
    """
    engine = get_engine()
    market_open = is_market_open()
    client = None
    try:
        client = get_schwab_client(interactive=False)
    except AuthenticationRequiredError:
        pass
    except Exception as e:
        logger.warning("Refresh: Schwab client init failed — %s", e)

    snapshot_updated = False
    collection_error: Optional[str] = None
    upstream_error = False
    symbols_failed: list[str] = []

    if market_open and client:
        try:
            summary = run_collection_cycle(client)
            symbols_failed = list(summary.get("symbols_failed") or [])
            if symbols_failed and not (summary["symbols_ok"] or summary["symbols_partial"]):
                collection_error = "collection_failed"
                upstream_error = True
            else:
                snapshot_updated = True
        except Exception as e:
            collection_error = str(e)
            upstream_error = "502" in str(e) or "upstream" in str(e).lower()

    freshness = check_data_freshness()
    last_id = freshness.get("last_snapshot_id")
    last_ts = freshness.get("last_snapshot_ts")
    has_snapshot = last_id is not None

    if last_ts is not None and hasattr(last_ts, "isoformat"):
        snapshot_ts_str = last_ts.isoformat().replace("+00:00", "Z")
    else:
        snapshot_ts_str = str(last_ts) if last_ts else None

    run_exit_scan(engine=engine, pricing_only=True)
    pricing_refreshed = has_snapshot

    if snapshot_updated:
        ok = True
        feed_status = "fresh"
        reason = "new_snapshot"
        message = "Prices refreshed from latest snapshot."
    elif pricing_refreshed:
        ok = True
        if upstream_error:
            feed_status = "upstream_error"
            reason = "using_last_good_snapshot"
            message = "Broker data unavailable right now (502). Last good prices retained."
        elif market_open and collection_error:
            feed_status = "delayed"
            reason = "using_last_good_snapshot"
            message = "No fresh snapshot available; repriced from last good snapshot."
        elif not market_open:
            feed_status = "market_closed"
            reason = "market_closed"
            message = "Market is closed; no fresh quotes available."
        else:
            feed_status = "delayed"
            reason = "using_last_good_snapshot"
            message = "No fresh snapshot available; repriced from last good snapshot."
    else:
        ok = False
        feed_status = "no_data"
        reason = "no_snapshot"
        message = "No usable snapshot and no fresh data."

    result: dict[str, Any] = {
        "ok": ok,
        "snapshot_updated": snapshot_updated,
        "pricing_refreshed": pricing_refreshed,
        "feed_status": feed_status,
        "reason": reason,
        "snapshot_id": last_id,
        "snapshot_ts": snapshot_ts_str,
        "message": message,
    }
    if symbols_failed:
        result["symbols_failed"] = symbols_failed
    return result


# ── Agent config ──────────────────────────────────────────────────────────────

@app.get("/config")
def get_all_config():
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT key, value, description FROM agent_config ORDER BY key")).fetchall()
    return {"config": {row.key: {"value": row.value, "description": row.description} for row in rows}}


@app.get("/config/{key}")
def get_config(key: str):
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT key, value, description, updated_at FROM agent_config WHERE key = :key"),
            {"key": key}
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config key '{key}' not found")
    return {"key": row.key, "value": row.value, "description": row.description, "updated_at": row.updated_at}


@app.post("/config/{key}")
def set_config(key: str, body: dict):
    value = body.get("value")
    if value is None:
        raise HTTPException(status_code=422, detail="body must contain 'value' field")
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO agent_config (key, value, updated_at)
                VALUES (:key, :value, NOW())
                ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = NOW()
            """),
            {"key": key, "value": str(value)}
        )
        conn.commit()
    return {"key": key, "value": str(value), "updated_at": "now"}


# ── Goals ─────────────────────────────────────────────────────────────────────

class GoalCreate(BaseModel):
    goal_type:  str = "monthly"
    goal_text:  str
    priority:   int = 10
    start_date: str          # ISO-8601 date string: "2026-03-23"
    end_date:   Optional[str] = None


class GoalStatusUpdate(BaseModel):
    status: str              # active | paused | completed | expired | cancelled


def _goal_row_to_dict(row) -> dict:
    d = dict(row._mapping)
    for k in ("start_date", "end_date"):
        if d.get(k) is not None:
            d[k] = str(d[k])
    for k in ("created_at", "updated_at"):
        if isinstance(d.get(k), datetime):
            d[k] = d[k].isoformat()
    return d


@app.get("/goals")
def list_goals(status: str = Query("active")):
    """
    List goals filtered by status.
    Default: status=active.  Pass status=all to return every row.
    """
    engine = get_engine()
    with engine.connect() as conn:
        if status == "all":
            rows = conn.execute(text("""
                SELECT id, goal_type, goal_text, priority, start_date, end_date,
                       status, created_at, updated_at
                FROM sparky_goals
                ORDER BY priority ASC, created_at DESC
            """)).fetchall()
        else:
            rows = conn.execute(text("""
                SELECT id, goal_type, goal_text, priority, start_date, end_date,
                       status, created_at, updated_at
                FROM sparky_goals
                WHERE status = :status
                ORDER BY priority ASC, created_at DESC
            """), {"status": status}).fetchall()
    return {"goals": [_goal_row_to_dict(r) for r in rows], "filter": status}


@app.post("/goals")
def create_goal(body: GoalCreate):
    """Create a new goal. Returns the created row."""
    valid_types    = {"weekly", "monthly", "temporary"}
    valid_statuses = {"active", "paused", "completed", "expired", "cancelled"}
    if body.goal_type not in valid_types:
        raise HTTPException(status_code=422,
            detail=f"goal_type must be one of {sorted(valid_types)}")
    if not body.goal_text.strip():
        raise HTTPException(status_code=422, detail="goal_text must not be empty")
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(text("""
            INSERT INTO sparky_goals
                (goal_type, goal_text, priority, start_date, end_date, status)
            VALUES
                (:goal_type, :goal_text, :priority, :start_date, :end_date, 'active')
            RETURNING id, goal_type, goal_text, priority, start_date, end_date,
                      status, created_at, updated_at
        """), {
            "goal_type":  body.goal_type,
            "goal_text":  body.goal_text.strip(),
            "priority":   body.priority,
            "start_date": body.start_date,
            "end_date":   body.end_date,
        }).fetchone()
    return {"goal": _goal_row_to_dict(row)}


@app.post("/goals/{goal_id}/status")
def update_goal_status(goal_id: int, body: GoalStatusUpdate):
    """Update a goal's status and refresh updated_at."""
    valid_statuses = {"active", "paused", "completed", "expired", "cancelled"}
    if body.status not in valid_statuses:
        raise HTTPException(status_code=422,
            detail=f"status must be one of {sorted(valid_statuses)}")
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(text("""
            UPDATE sparky_goals
            SET status     = :status,
                updated_at = now()
            WHERE id = :id
            RETURNING id, goal_type, goal_text, priority, start_date, end_date,
                      status, created_at, updated_at
        """), {"status": body.status, "id": goal_id}).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Goal id={goal_id} not found")
    return {"goal": _goal_row_to_dict(row)}


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

    # Schwab token — refresh tokens expire exactly 7 days after creation_timestamp.
    # token.json has no top-level expiry field; compute from creation_timestamp.
    try:
        token_path = PROJECT_ROOT / "token.json"
        if token_path.exists():
            token_data = json.loads(token_path.read_text())
            creation_ts = token_data.get("creation_timestamp")
            if creation_ts:
                exp_dt    = datetime.fromtimestamp(creation_ts + 7 * 86400, tz=timezone.utc)
                days_left = (exp_dt - datetime.now(timezone.utc)).total_seconds() / 86400
                health["token"] = {
                    "valid":          days_left > 0,
                    "days_remaining": round(days_left, 1),
                    "expires_at":     exp_dt.isoformat(),
                }
            else:
                # No usable timestamp — cannot determine expiry; not a hard failure
                health["token"] = "expiry_unknown"
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


# ── Performance ────────────────────────────────────────────────────────────────

BACKEND_LOG = PROJECT_ROOT / "backend.log"

# ── Watchlist screener cache (15-minute TTL) ──────────────────────────────────
_screener_cache: dict | None = None
_screener_cache_ts: datetime | None = None
_SCREENER_CACHE_TTL_SECONDS = 900  # 15 minutes


def _get_reconciler_error_days() -> set:
    """
    Parse RECONCILER_LOG line by line. Return set of date objects where at
    least one JSON line recorded reconciler errors (errors list non-empty or
    errors integer > 0). Returns empty set if file missing or unreadable.
    """
    error_days: set = set()
    if not RECONCILER_LOG.exists():
        return error_days
    try:
        for line in RECONCILER_LOG.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except Exception:
                continue
            ts_str = parsed.get("ts")
            if not ts_str:
                continue
            try:
                ts_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                day = ts_dt.date()
            except Exception:
                continue
            # errors lives under positions.errors (list) or top-level errors (int)
            positions = parsed.get("positions") or {}
            errors_val = positions.get("errors")
            if errors_val is None:
                errors_val = parsed.get("errors")
            if errors_val is None:
                continue
            if isinstance(errors_val, list):
                has_error = len(errors_val) > 0
            elif isinstance(errors_val, (int, float)):
                has_error = errors_val > 0
            else:
                has_error = bool(errors_val)
            if has_error:
                error_days.add(day)
    except Exception as e:
        logger.warning("_get_reconciler_error_days: could not parse reconciler.log — %s", e)
    return error_days


def _count_clean_paper_days(conn) -> int:
    """
    Count distinct calendar dates passing all three clean day conditions:
      1. At least one snapshot_run with status IN ('ok','partial') that date.
      2. No undismissed STOP_LOSS/STOP_TRIGGERED exit_signals for that date.
      3. No reconciler errors logged for that date.
    """
    # Condition 1: dates with a successful snapshot run
    snap_rows = conn.execute(text("""
        SELECT DISTINCT DATE(ts) AS snap_date
        FROM snapshot_runs
        WHERE status IN ('ok', 'partial')
    """)).fetchall()
    candidate_dates = {r[0] for r in snap_rows}

    if not candidate_dates:
        return 0

    # Condition 2: remove dates that had an undismissed stop-loss signal
    stop_rows = conn.execute(text("""
        SELECT DISTINCT DATE(created_at) AS sig_date
        FROM exit_signals
        WHERE reason IN ('STOP_LOSS', 'STOP_TRIGGERED')
          AND status != 'dismissed'
          AND account_id = 'PAPER'
    """)).fetchall()
    candidate_dates -= {r[0] for r in stop_rows}

    if not candidate_dates:
        return 0

    # Condition 3: remove dates with reconciler errors
    candidate_dates -= _get_reconciler_error_days()

    return len(candidate_dates)


@app.get("/performance")
def get_performance():
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    now_utc = datetime.now(timezone.utc)
    generated_at = now_utc.isoformat()
    today_et = now_utc.astimezone(ET).strftime("%Y-%m-%d")

    log_dir = PROJECT_ROOT / "logs" / "daily"
    snapshot_path = log_dir / f"performance_snapshot_{today_et}.json"

    def _write_snapshot(data: dict) -> None:
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path.write_text(json.dumps(data, indent=2))
            with BACKEND_LOG.open("a") as f:
                f.write(f"Performance snapshot written: {snapshot_path}\n")
        except Exception as exc:
            logger.error("get_performance: failed to write snapshot — %s", exc)
            try:
                with BACKEND_LOG.open("a") as f:
                    f.write(f"Performance snapshot write failed: {exc}\n")
            except Exception:
                pass

    def _error_response(msg: str) -> dict:
        return {
            "status": "error",
            "generated_at": generated_at,
            "metrics": {
                "total_trades": None,
                "wins": None,
                "losses": None,
                "win_rate": None,
                "avg_credit_collected": None,
                "avg_exit_cost": None,
                "total_pnl": None,
                "avg_hold_days": None,
                "best_trade": None,
                "worst_trade": None,
            },
            "stage_2_eligibility": {
                "eligible": False,
                "reason": "DB unreachable.",
                "checks": {
                    "clean_paper_days": {"pass": False, "value": 0, "required": 5},
                    "positive_pnl": {"pass": False, "value": 0.0},
                    "no_stop_losses": {"pass": False, "stop_loss_count": 0},
                },
            },
        }

    try:
        engine = get_engine()
        with engine.connect() as conn:
            # ── Base query: closed approved paper trades ──────────────────────
            rows = conn.execute(text("""
                SELECT
                    tc.id,
                    tc.symbol,
                    tc.created_at        AS candidate_created_at,
                    td.decided_at        AS approved_at,
                    to_.entry_credit,
                    to_.exit_debit,
                    to_.pnl
                FROM trade_outcomes to_
                JOIN trade_decisions td  ON td.id  = to_.decision_id
                JOIN trade_candidates tc ON tc.id  = td.candidate_id
                WHERE td.decision = 'approved'
                  AND to_.account_id = 'PAPER'
                ORDER BY td.decided_at ASC
            """)).fetchall()

            total_trades = len(rows)

            # ── Stage 2 checks (computed regardless of trade count) ───────────
            clean_days_count = _count_clean_paper_days(conn)
            stop_loss_count = int(conn.execute(text("""
                SELECT COUNT(*) FROM exit_signals
                WHERE reason IN ('STOP_LOSS', 'STOP_TRIGGERED')
                  AND status != 'dismissed'
                  AND account_id = 'PAPER'
            """)).scalar() or 0)

            if total_trades == 0:
                response = {
                    "status": "no_data",
                    "generated_at": generated_at,
                    "metrics": {
                        "total_trades": 0,
                        "wins": None,
                        "losses": None,
                        "win_rate": None,
                        "avg_credit_collected": None,
                        "avg_exit_cost": None,
                        "total_pnl": None,
                        "avg_hold_days": None,
                        "best_trade": None,
                        "worst_trade": None,
                    },
                    "stage_2_eligibility": {
                        "eligible": False,
                        "reason": "No closed paper trades yet.",
                        "checks": {
                            "clean_paper_days": {
                                "pass": clean_days_count >= 5,
                                "value": clean_days_count,
                                "required": 5,
                            },
                            "positive_pnl": {"pass": False, "value": 0.0},
                            "no_stop_losses": {
                                "pass": stop_loss_count == 0,
                                "stop_loss_count": stop_loss_count,
                            },
                        },
                    },
                }
                _write_snapshot(response)
                return response

            # ── Metrics ───────────────────────────────────────────────────────
            wins   = sum(1 for r in rows if r.pnl is not None and float(r.pnl) > 0)
            losses = total_trades - wins

            win_rate = round(wins / total_trades * 100, 1)

            credits = [float(r.entry_credit) for r in rows if r.entry_credit is not None]
            exits   = [float(r.exit_debit)   for r in rows if r.exit_debit   is not None]
            pnls    = [float(r.pnl)           for r in rows if r.pnl          is not None]

            avg_credit   = round(sum(credits) / len(credits), 4) if credits else None
            avg_exit     = round(sum(exits)   / len(exits),   4) if exits   else None
            total_pnl    = round(sum(pnls), 2)                   if pnls    else 0.0

            hold_days_list = []
            for r in rows:
                if r.approved_at is not None and r.candidate_created_at is not None:
                    approved = r.approved_at
                    created  = r.candidate_created_at
                    if hasattr(approved, "tzinfo") and approved.tzinfo is None:
                        approved = approved.replace(tzinfo=timezone.utc)
                    if hasattr(created, "tzinfo") and created.tzinfo is None:
                        created = created.replace(tzinfo=timezone.utc)
                    hold_days_list.append((approved - created).total_seconds() / 86400)
            avg_hold_days = round(sum(hold_days_list) / len(hold_days_list), 1) if hold_days_list else None

            def _trade_dict(row) -> dict:
                dt = row.approved_at
                if dt is not None and hasattr(dt, "date"):
                    date_str = dt.date().isoformat()
                elif dt is not None:
                    date_str = str(dt)[:10]
                else:
                    date_str = None
                return {
                    "symbol": row.symbol,
                    "pnl":    round(float(row.pnl), 2) if row.pnl is not None else None,
                    "date":   date_str,
                }

            rows_with_pnl = [r for r in rows if r.pnl is not None]
            best_trade  = _trade_dict(max(rows_with_pnl, key=lambda r: float(r.pnl))) if rows_with_pnl else None
            worst_trade = _trade_dict(min(rows_with_pnl, key=lambda r: float(r.pnl))) if rows_with_pnl else None

            # ── Stage 2 eligibility ───────────────────────────────────────────
            check_1_pass = clean_days_count >= 5
            check_2_pass = total_pnl > 0
            check_3_pass = stop_loss_count == 0
            eligible = check_1_pass and check_2_pass and check_3_pass

            if eligible:
                reason = "All criteria met. Ready for live review."
            elif not check_1_pass:
                reason = f"Clean paper days: {clean_days_count}/5 required."
            elif not check_2_pass:
                reason = (
                    f"Total P&L is negative (${total_pnl}). "
                    "Positive P&L required before live trading."
                )
            else:
                reason = (
                    f"Stop-loss triggered {stop_loss_count} time(s). "
                    "Review required before live trading."
                )

            response = {
                "status": "ok",
                "generated_at": generated_at,
                "metrics": {
                    "total_trades":         total_trades,
                    "wins":                 wins,
                    "losses":               losses,
                    "win_rate":             win_rate,
                    "avg_credit_collected": avg_credit,
                    "avg_exit_cost":        avg_exit,
                    "total_pnl":            total_pnl,
                    "avg_hold_days":        avg_hold_days,
                    "best_trade":           best_trade,
                    "worst_trade":          worst_trade,
                },
                "stage_2_eligibility": {
                    "eligible": eligible,
                    "reason":   reason,
                    "checks": {
                        "clean_paper_days": {
                            "pass":     check_1_pass,
                            "value":    clean_days_count,
                            "required": 5,
                        },
                        "positive_pnl": {
                            "pass":  check_2_pass,
                            "value": total_pnl,
                        },
                        "no_stop_losses": {
                            "pass":            check_3_pass,
                            "stop_loss_count": stop_loss_count,
                        },
                    },
                },
            }
            _write_snapshot(response)
            return response

    except Exception as e:
        logger.error("get_performance: unhandled error — %s", e)
        response = _error_response(str(e))
        _write_snapshot(response)
        return response


# ── Watchlist screener ─────────────────────────────────────────────────────────

@app.get("/screener")
def get_screener(refresh: bool = Query(False, description="Force cache bypass")):
    """
    Return the latest watchlist screener results.
    Results are cached for 15 minutes. Pass ?refresh=true to force a fresh run.
    """
    global _screener_cache, _screener_cache_ts

    now = datetime.now(timezone.utc)

    # Serve from cache if still fresh and not forced
    if (
        not refresh
        and _screener_cache is not None
        and _screener_cache_ts is not None
        and (now - _screener_cache_ts).total_seconds() < _SCREENER_CACHE_TTL_SECONDS
    ):
        age_seconds = int((now - _screener_cache_ts).total_seconds())
        return {**_screener_cache, "cached": True, "cache_age_seconds": age_seconds}

    # Run screener
    result = run_screener()
    _screener_cache = result
    _screener_cache_ts = now

    return {**result, "cached": False, "cache_age_seconds": 0}


# ── Gate rule labels for /shadow gate_kill_distribution ───────────────────────

_GATE_RULE_LABELS: dict[str, str] = {
    "max_open_condors":   "Max Open Positions",
    "daily_loss_kill":    "Daily Loss Kill Switch",
    "net_credit":         "Net Credit Minimum",
    "short_delta":        "Short Delta Limit",
    "fomc_proximity":     "FOMC Proximity",
    "earnings_proximity": "Earnings Overlap",
    "position_risk":      "Position Risk vs NAV",
    "correlated_risk":    "Correlated Risk",
    "underlying_volume":  "Underlying Volume (ADV)",
    "open_interest":      "Open Interest / Liquidity",
    "iv_rank":            "IV Rank",
}


def _gate_kill_label(rule_key: Optional[str]) -> str:
    if not rule_key:
        return "Unknown / Legacy"
    k = str(rule_key).strip()
    if k == _UNKNOWN_LEGACY_RULE:
        return "Unknown / Legacy"
    if k in _GATE_RULE_LABELS:
        return _GATE_RULE_LABELS[k]
    k_norm = k.lower().replace(" ", "_").replace("-", "_")
    if k_norm in _GATE_RULE_LABELS:
        return _GATE_RULE_LABELS[k_norm]
    return k_norm.replace("_", " ").title()


# ── Pipeline stats ───────────────────────────────────────────────────────────

@app.get("/pipeline-stats")
def get_pipeline_stats(
    hours: int = Query(48, ge=1, le=168, description="Look-back window in hours (1–168)"),
):
    """
    Aggregate candidate/decision counts for the Shadow funnel UI.
    All counts are scoped to trade_candidates.created_at >= cutoff unless noted.
    """
    from datetime import timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("""
            SELECT
              (SELECT COUNT(*)::bigint FROM trade_candidates tc WHERE tc.created_at >= :cutoff)
                AS scanned,
              (SELECT COUNT(*)::bigint FROM trade_candidates tc
               WHERE tc.created_at >= :cutoff AND tc.gate_result = 'blocked')
                AS blocked,
              (SELECT COUNT(*)::bigint FROM trade_candidates tc
               WHERE tc.created_at >= :cutoff AND tc.gate_result = 'approved')
                AS passed_gates,
              (SELECT COUNT(*)::bigint FROM trade_candidates tc
               WHERE tc.created_at >= :cutoff
                 AND tc.gate_result = 'approved'
                 AND tc.llm_card ? 'recommendation')
                AS llm_evaluated,
              (SELECT COUNT(*)::bigint FROM trade_decisions td
               JOIN trade_candidates tc ON tc.id = td.candidate_id
               WHERE tc.created_at >= :cutoff
                 AND LOWER(TRIM(td.decision)) = 'approved')
                AS approved,
              (SELECT COUNT(*)::bigint FROM trade_decisions td
               JOIN trade_candidates tc ON tc.id = td.candidate_id
               WHERE tc.created_at >= :cutoff
                 AND LOWER(TRIM(td.decision)) = 'rejected')
                AS rejected,
              (SELECT COUNT(*)::bigint FROM trade_candidates tc
               WHERE tc.created_at >= :cutoff
                 AND tc.gate_result = 'approved'
                 AND COALESCE(tc.llm_card->>'approval_status', '') = 'expired')
                AS expired,
              (SELECT COUNT(*)::bigint FROM trade_candidates tc
               WHERE tc.created_at >= :cutoff
                 AND tc.gate_result = 'approved'
                 AND tc.llm_card ? 'recommendation'
                 AND NOT EXISTS (
                   SELECT 1 FROM trade_decisions td WHERE td.candidate_id = tc.id
                 ))
                AS awaiting_operator_decision
        """),
            {"cutoff": cutoff},
        ).mappings().first()

    scanned = int(row["scanned"] or 0) if row else 0
    blocked = int(row["blocked"] or 0) if row else 0
    passed_gates = int(row["passed_gates"] or 0) if row else 0
    llm_evaluated = int(row["llm_evaluated"] or 0) if row else 0
    approved = int(row["approved"] or 0) if row else 0
    rejected = int(row["rejected"] or 0) if row else 0
    expired = int(row["expired"] or 0) if row else 0
    awaiting_operator_decision = int(row["awaiting_operator_decision"] or 0) if row else 0

    circuit_broken = max(0, passed_gates - llm_evaluated)

    return {
        "scanned":                     scanned,
        "passed_gates":                passed_gates,
        "blocked":                     blocked,
        "llm_evaluated":               llm_evaluated,
        "circuit_broken":              circuit_broken,
        "approved":                    approved,
        "rejected":                    rejected,
        "expired":                     expired,
        "awaiting_operator_decision":  awaiting_operator_decision,
        "hours":                       hours,
        "cutoff":                      cutoff.isoformat(),
    }


# ── Shadow mode — blocked candidates ──────────────────────────────────────────

@app.get("/shadow")
def get_shadow(
    hours: int = Query(48, ge=1, le=168, description="Look-back window in hours (1–168)"),
):
    """
    Return candidates that failed the rules gate (gate_result='blocked'),
    ordered newest-first, limited to the last N hours (default 48, max 168).

    Each row exposes: id, symbol, score, net_credit, expiry, blocked_reason,
    created_at, snapshot_id.  blocked_reason is a JSONB object with keys
    'rule' (the failing rule name) and 'detail' (human-readable explanation).
    """
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                id,
                symbol,
                score,
                strategy,
                candidate_json,
                candidate_json->>'net_credit' AS net_credit,
                candidate_json->>'expiry'     AS expiry,
                blocked_reason,
                created_at,
                snapshot_id
            FROM trade_candidates
            WHERE gate_result = 'blocked'
              AND created_at  >= :cutoff
            ORDER BY created_at DESC
            LIMIT 100
        """), {"cutoff": cutoff}).fetchall()

        dist_rows = conn.execute(
            text("""
            SELECT
              COALESCE(
                NULLIF(TRIM(blocked_reason->>'rule'), ''),
                :unknown
              ) AS rule,
              COUNT(*)::bigint AS cnt
            FROM trade_candidates
            WHERE gate_result = 'blocked'
              AND created_at >= :cutoff
            GROUP BY 1
            ORDER BY cnt DESC
        """),
            {"cutoff": cutoff, "unknown": _UNKNOWN_LEGACY_RULE},
        ).fetchall()

    results = []
    for row in rows:
        d = _serialize(row)
        raw_cj = d.pop("candidate_json", None)
        cj = _parse_jsonb(raw_cj)
        d["blocked_reason"] = _parse_jsonb(d.get("blocked_reason"))
        br = d["blocked_reason"]
        d["gate_rule_label"] = _gate_kill_label(_blocked_rule_bucket_key(br))
        for k, v in _flatten_condor_spread_fields(cj, d.get("strategy")).items():
            d[k] = v
        results.append(d)

    gate_kill_distribution = [
        {
            "rule":  r[0],
            "label": _gate_kill_label(r[0]),
            "count": int(r[1]),
        }
        for r in dist_rows
    ]

    return {
        "blocked":                results,
        "count":                  len(results),
        "hours":                  hours,
        "cutoff":                 cutoff.isoformat(),
        "gate_kill_distribution": gate_kill_distribution,
    }


# ── History ───────────────────────────────────────────────────────────────────

@app.get("/history")
def get_history(
    days: int = Query(30, ge=1, description="Look-back window in days (anchored on td.decided_at)"),
):
    """
    Return decision history — every trade_candidates row that has a
    trade_decisions row — joined with trade_outcomes for P&L.

    Filter anchor: td.decided_at (when the operator made a decision).
    Defaults to last 30 days; max 90 days (silently clamped).
    Returns at most 200 rows, ordered newest-first.

    Each row: id, symbol, score, account_id, created_at, net_credit, expiry,
              decision, decided_at, reason, pnl, exit_reason, closed_at.
    pnl and closed_at are null for rejected or still-open trades.
    """
    days = min(days, 90)

    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                tc.id,
                tc.symbol,
                tc.score,
                tc.strategy,
                tc.account_id,
                tc.created_at,
                tc.candidate_json,
                tc.llm_card,
                tc.blocked_reason,
                td.decision,
                td.decided_at,
                td.reason,
                to2.pnl,
                to2.exit_reason,
                to2.closed_at
            FROM trade_candidates tc
            JOIN trade_decisions td ON td.candidate_id = tc.id
            LEFT JOIN trade_outcomes to2 ON to2.decision_id = td.id
            WHERE td.decided_at >= NOW() - (:days * INTERVAL '1 day')
            ORDER BY tc.id DESC
            LIMIT 200
        """), {"days": days}).fetchall()

    results = []
    for row in rows:
        d = _serialize(row)
        cj = _parse_jsonb(d.pop("candidate_json", None))
        d["net_credit"] = cj.get("net_credit")
        d["expiry"]     = cj.get("expiry")
        for k, v in _flatten_condor_spread_fields(cj, d.get("strategy")).items():
            d[k] = v

        lc = _parse_jsonb(d.pop("llm_card", None))
        br = d.pop("blocked_reason", None)
        gd = _parse_jsonb(br) if br is not None else None
        d["gate_diagnostics"] = gd if gd else None

        if isinstance(lc, dict) and lc:
            meta = lc.get("_meta") or lc.get("meta")
            if not isinstance(meta, dict):
                meta = {}
            d["llm_recommendation"] = lc.get("recommendation")
            d["llm_confidence"]     = lc.get("confidence")
            summ = lc.get("summary")
            d["llm_reasoning"]      = summ if summ is not None else lc.get("reasoning")
            d["llm_model"]          = meta.get("model")
            d["llm_latency"]        = meta.get("latency")
        else:
            d["llm_recommendation"] = None
            d["llm_confidence"]     = None
            d["llm_reasoning"]      = None
            d["llm_model"]          = None
            d["llm_latency"]        = None

        results.append(d)

    return {"history": results, "count": len(results), "days": days}