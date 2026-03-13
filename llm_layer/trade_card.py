"""
llm_layer/trade_card.py
Generates structured LLM trade cards for approved iron condor candidates.

Flow:
  trade_candidates (gate_result='approved', llm_card empty)
    → build prompt (system + trade template + retrieval context)
    → Ollama llama3.2:3b with format='json'
    → validate_trade_card()
    → write back to trade_candidates.llm_card (JSONB)

Circular import note:
  retrieval is imported lazily (inside functions) to avoid the
  circular init chain at module load time.
  Do NOT add a top-level 'from llm_layer.retrieval import ...' here.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from sqlalchemy import create_engine, text

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from llm_layer.validator import validate_trade_card

logger = logging.getLogger(__name__)

DB_URL      = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
OLLAMA_URL  = "http://localhost:11434/api/generate"
MODEL       = "llama3.2:3b"
PROMPTS_DIR = Path("llm_layer/prompts")


# ── Prompt helpers ───────────────────────────────────────────────────────────

def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8").strip()


def _build_prompt(candidate: Dict[str, Any], context_block: str) -> str:
    template = _read(PROMPTS_DIR / "trade_card.txt")

    sym   = candidate.get("symbol") or candidate.get("underlying", "UNKNOWN")
    price = candidate.get("underlying_price", candidate.get("price", "N/A"))
    exp   = candidate.get("expiry", "N/A")
    dte   = candidate.get("dte", "N/A")

    sp_strike = candidate.get("short_put_strike", "N/A")
    sp_delta  = candidate.get("short_put_delta",  "N/A")
    lp_strike = candidate.get("long_put_strike",  "N/A")
    lp_delta  = candidate.get("long_put_delta",   "N/A")
    sc_strike = candidate.get("short_call_strike", "N/A")
    sc_delta  = candidate.get("short_call_delta",  "N/A")
    lc_strike = candidate.get("long_call_strike",  "N/A")
    lc_delta  = candidate.get("long_call_delta",   "N/A")

    credit   = candidate.get("net_credit",   "N/A")
    max_loss = candidate.get("max_loss",     "N/A")
    width    = candidate.get("spread_width", "N/A")
    score    = candidate.get("total_score",  candidate.get("score", "N/A"))

    breakdown = candidate.get("score_breakdown") or {
        "iv_rank_score":      candidate.get("iv_rank_score",      "N/A"),
        "credit_width_score": candidate.get("credit_width_score", "N/A"),
        "delta_score":        candidate.get("delta_score",        "N/A"),
        "dte_score":          candidate.get("dte_score",          "N/A"),
    }

    return template.format(
        symbol            = sym,
        underlying_price  = price,
        expiry            = exp,
        dte               = dte,
        short_put_strike  = sp_strike,
        short_put_delta   = sp_delta,
        long_put_strike   = lp_strike,
        long_put_delta    = lp_delta,
        short_call_strike = sc_strike,
        short_call_delta  = sc_delta,
        long_call_strike  = lc_strike,
        long_call_delta   = lc_delta,
        net_credit        = credit,
        max_loss          = max_loss,
        width             = width,
        score             = score,
        score_breakdown   = json.dumps(breakdown, indent=2),
        retrieval_context = context_block or "Context unavailable",
    )


# ── Ollama ───────────────────────────────────────────────────────────────────

def call_ollama(system: str, prompt: str) -> str:
    payload = {
        "model":   MODEL,
        "system":  system,
        "prompt":  prompt,
        "stream":  False,
        "format":  "json",
        "options": {"temperature": 0.2},
    }
    r = httpx.post(OLLAMA_URL, json=payload, timeout=90)
    r.raise_for_status()
    return r.json().get("response", "").strip()


# ── DB helpers ───────────────────────────────────────────────────────────────

def fetch_latest_approved_candidate(conn) -> Optional[Dict[str, Any]]:
    row = conn.execute(text("""
        SELECT id, account_id, candidate_json, score
        FROM trade_candidates
        WHERE gate_result = 'approved'
          AND (
            llm_card IS NULL
            OR llm_card = '{}'::jsonb
            OR NOT (llm_card ? 'recommendation')
          )
        ORDER BY id DESC
        LIMIT 1
    """)).fetchone()
    if not row:
        return None
    cid, account_id, candidate_json, score = row
    candidate_obj = (
        json.loads(candidate_json)
        if isinstance(candidate_json, str)
        else candidate_json
    )
    # Inject DB score column so _build_prompt can find it
    candidate_obj["total_score"] = score
    return {"id": cid, "account_id": account_id, "candidate": candidate_obj}


def write_llm_card(conn, trade_candidate_id: int, card: Dict[str, Any]) -> None:
    conn.execute(text("""
        UPDATE trade_candidates
        SET llm_card = cast(:card as jsonb)
        WHERE id = :id
    """), {"id": trade_candidate_id, "card": json.dumps(card)})


# ── Main entry points ────────────────────────────────────────────────────────

def generate_one() -> None:
    from llm_layer.retrieval import build_context_block
    from llm_layer import circuit_breaker as cb_module  # add this

    cb = cb_module.get()                                # add this
    if cb.is_open():                                    # add this
        logger.warning(f"LLM circuit open — skipping. {cb.status_line()}")
        return

    system_prompt = _read(PROMPTS_DIR / "system.txt")
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        item = fetch_latest_approved_candidate(conn)
        if not item:
            logger.info("No approved candidates needing LLM cards.")
            return

        trade_id   = item["id"]
        account_id = item["account_id"]
        candidate  = item["candidate"]
        symbol     = candidate.get("symbol") or "UNKNOWN"

        context_block = build_context_block(symbol=symbol, account_id=account_id)
        prompt        = _build_prompt(candidate, context_block)

        logger.info(f"Generating trade card: id={trade_id} symbol={symbol}")

        try:                                            # add this
            raw  = call_ollama(system_prompt, prompt)
            card = validate_trade_card(raw)
            cb.record_success()                         # add this
        except Exception as e:                          # add this
            cb.record_failure(str(e))                   # add this
            raise                                       # add this

        card["_meta"] = {
            "model":              MODEL,
            "trade_candidate_id": trade_id,
            "context_block":      context_block,
        }

        write_llm_card(conn, trade_id, card)
        logger.info(
            f"Card written: id={trade_id} symbol={symbol} "
            f"rec={card.get('recommendation')} conf={card.get('confidence')}"
        )

def generate_all_pending() -> int:
    from llm_layer.retrieval import build_context_block

    system_prompt = _read(PROMPTS_DIR / "system.txt")
    engine = create_engine(DB_URL)
    written = 0

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, account_id, candidate_json, score
            FROM trade_candidates
            WHERE gate_result = 'approved'
              AND (
                llm_card IS NULL
                OR llm_card = '{}'::jsonb
                OR NOT (llm_card ? 'recommendation')
              )
            ORDER BY id DESC
        """)).fetchall()

        if not rows:
            logger.info("No pending candidates.")
            return 0

        for row in rows:
            cid, account_id, candidate_json, score = row
            candidate = (
                json.loads(candidate_json)
                if isinstance(candidate_json, str)
                else candidate_json
            )
            candidate["total_score"] = score
            symbol = candidate.get("symbol") or "UNKNOWN"
            try:
                context_block = build_context_block(symbol=symbol, account_id=account_id)
                prompt        = _build_prompt(candidate, context_block)
                raw           = call_ollama(system_prompt, prompt)
                card          = validate_trade_card(raw)
                card["_meta"] = {"model": MODEL, "trade_candidate_id": cid}
                write_llm_card(conn, cid, card)
                logger.info(f"Card written: id={cid} symbol={symbol} rec={card.get('recommendation')}")
                written += 1
            except Exception as e:
                logger.error(f"Card failed: id={cid} symbol={symbol} — {e}")

    return written


# ── Manual test ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    generate_one()