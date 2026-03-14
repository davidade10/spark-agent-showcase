"""
execution/executor.py — Phase 5: Execution Layer

Routes approved iron condor candidates to paper simulation (dry_run) or
raises NotImplementedError for live until Phase 5 live sign-off is done.

TRADING_MODE is read from .env — defaults to 'paper'.
Live execution is intentionally stubbed. No Schwab order API calls exist here.
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    HARD_RULES, TRADING_MODE,
)
from execution.order_state import migrate_orders_schema

load_dotenv()

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Environment ───────────────────────────────────────────────────────────────
# SCHWAB_ACCOUNT_HASH: the encrypted account identifier returned by
# GET /accounts — used in POST /accounts/{accountHash}/orders.
# NOT the visible account number shown in the Schwab UI.
SCHWAB_ACCOUNT_HASH = os.getenv("SCHWAB_ACCOUNT_HASH", "")
PAPER_ACCOUNT_NAV   = float(os.getenv("PAPER_ACCOUNT_NAV", "20000"))
LIVE_ACCOUNT_NAV    = float(os.getenv("LIVE_ACCOUNT_NAV",  "14836"))

# ── Safety check — always visible in logs ────────────────────────────────────
if TRADING_MODE == "live":
    logger.warning(
        "TRADING_MODE=live — real orders will be sent to Schwab. "
        "Complete Phase 5 live sign-off before enabling live execution."
    )

# ── Payload builder ───────────────────────────────────────────────────────────
def build_iron_condor_payload(candidate_json: dict, quantity: int) -> dict:
    """
    Constructs the four-leg Schwab order dict for an iron condor.
    candidate_json is the raw dict from the trade_candidates row.

    complexOrderStrategyType='IRON_CONDOR' with legs in order:
      1. BUY_TO_OPEN  long put   (protection wing)
      2. SELL_TO_OPEN short put  (premium)
      3. SELL_TO_OPEN short call (premium)
      4. BUY_TO_OPEN  long call  (protection wing)

    OCC symbol format: SYMBOL(6) + YYMMDD + right(1) + strike(8 digits, x1000)
    Example: SPY   260417P00540000
    """
    symbol = candidate_json["symbol"]
    expiry = candidate_json["expiry"].replace("-", "")   # "20260417"
    yymmdd = expiry[2:]                                   # "260417"

    def _occ(strike: float, right: str) -> str:
        strike_str = f"{int(round(strike * 1000)):08d}"
        return f"{symbol:<6}{yymmdd}{right}{strike_str}"

    legs = [
        {
            "instruction": "BUY_TO_OPEN",
            "quantity":    quantity,
            "instrument":  {"assetType": "OPTION", "symbol": _occ(candidate_json["long_put_strike"],   "P")},
        },
        {
            "instruction": "SELL_TO_OPEN",
            "quantity":    quantity,
            "instrument":  {"assetType": "OPTION", "symbol": _occ(candidate_json["short_put_strike"],  "P")},
        },
        {
            "instruction": "SELL_TO_OPEN",
            "quantity":    quantity,
            "instrument":  {"assetType": "OPTION", "symbol": _occ(candidate_json["short_call_strike"], "C")},
        },
        {
            "instruction": "BUY_TO_OPEN",
            "quantity":    quantity,
            "instrument":  {"assetType": "OPTION", "symbol": _occ(candidate_json["long_call_strike"],  "C")},
        },
    ]

    return {
        "orderType":                "NET_CREDIT",
        "session":                  "NORMAL",
        "duration":                 "DAY",
        "complexOrderStrategyType": "IRON_CONDOR",
        "quantity":                 quantity,
        "price":                    round(float(candidate_json["net_credit"]), 2),
        "orderLegCollection":       legs,
    }


# ── Quantity calculator ───────────────────────────────────────────────────────
def _compute_quantity(candidate_json: dict) -> int:
    """
    Quantity sizing via 6% max position risk rule:

        spread_width          = candidate_json['spread_width']  (already computed)
        max_loss_per_contract = (spread_width - net_credit) * 100
        max_risk_dollars      = NAV * max_position_risk_pct  (0.06)
        quantity              = floor(max_risk_dollars / max_loss_per_contract)
        minimum               = 1

    Uses PAPER_ACCOUNT_NAV when TRADING_MODE != 'live', else LIVE_ACCOUNT_NAV.
    """
    nav          = PAPER_ACCOUNT_NAV if TRADING_MODE != "live" else LIVE_ACCOUNT_NAV
    net_credit   = float(candidate_json["net_credit"])
    spread_width = float(candidate_json["spread_width"])   # max(put_spread, call_spread), always > 0

    max_loss_per_contract = (spread_width - net_credit) * 100
    max_risk_dollars      = nav * HARD_RULES["max_position_risk_pct"]

    if max_loss_per_contract <= 0:
        logger.warning(
            f"max_loss_per_contract={max_loss_per_contract:.4f} ≤ 0 "
            f"(spread_width={spread_width}, net_credit={net_credit}) — defaulting to 1 contract"
        )
        return 1

    return max(1, math.floor(max_risk_dollars / max_loss_per_contract))


# ── Main entry point ──────────────────────────────────────────────────────────
def execute_approved_candidate(candidate_id: int) -> int:
    """
    Executes an approved iron condor candidate.

    Steps:
      1. Read trade_candidate row from DB by id
      2. Confirm llm_card->>'approval_status' == 'approved'
      3. Extract candidate_json fields (symbol, strikes, expiry, net_credit)
      4. Compute quantity from 6% risk rule
      5. Insert pending row into orders table → get order_id
      6. Route to dry_run.simulate_fill (paper) or raise NotImplementedError (live)
      7. Return order_id

    Returns:
      order_id (int)

    Raises:
      ValueError           — candidate not found or not approved
      NotImplementedError  — TRADING_MODE=live (not yet enabled)
    """
    # Local import: dry_run only needed in paper path, avoids any circular risk
    from execution.dry_run import simulate_fill  # noqa: PLC0415

    engine = create_engine(DB_URL)
    migrate_orders_schema(engine)

    with engine.begin() as conn:
        # 1. Load trade_candidate row
        row = conn.execute(text("""
            SELECT id, symbol, candidate_json, llm_card, account_id
            FROM trade_candidates
            WHERE id = :id
        """), {"id": candidate_id}).fetchone()

        if not row:
            raise ValueError(f"trade_candidate id={candidate_id} not found")

        # 2. Confirm approval_status
        llm_card = (
            row.llm_card
            if isinstance(row.llm_card, dict)
            else json.loads(row.llm_card or "{}")
        )
        approval_status = llm_card.get("approval_status")
        if approval_status != "approved":
            raise ValueError(
                f"candidate id={candidate_id} has approval_status='{approval_status}'"
                " — must be 'approved' before execution"
            )

        # 3. Extract candidate_json
        candidate_json = (
            row.candidate_json
            if isinstance(row.candidate_json, dict)
            else json.loads(row.candidate_json or "{}")
        )

        symbol     = candidate_json["symbol"]
        account_id = row.account_id or "primary"

        # 4. Compute quantity
        quantity = _compute_quantity(candidate_json)

        logger.info(
            f"execute_approved_candidate: id={candidate_id} symbol={symbol} "
            f"qty={quantity} mode={TRADING_MODE} "
            f"nav={PAPER_ACCOUNT_NAV if TRADING_MODE != 'live' else LIVE_ACCOUNT_NAV:.0f}"
        )

        # 5. Insert pending order row
        payload = build_iron_condor_payload(candidate_json, quantity)
        result  = conn.execute(text("""
            INSERT INTO orders (
                candidate_id, account_id, symbol,
                status, source, order_payload, quantity, created_at
            ) VALUES (
                :candidate_id, :account_id, :symbol,
                'pending', :source, cast(:payload as jsonb), :quantity, :created_at
            )
            RETURNING id
        """), {
            "candidate_id": candidate_id,
            "account_id":   account_id,
            "symbol":       symbol,
            "source":       TRADING_MODE,
            "payload":      json.dumps(payload),
            "quantity":     quantity,
            "created_at":   datetime.now(timezone.utc),
        })
        order_id = result.scalar()

    # 6. Route — engine.begin() above has already committed the pending row
    if TRADING_MODE != "live":
        simulate_fill(candidate_json, quantity, order_id)
        logger.info(f"Paper fill complete — order_id={order_id} symbol={symbol}")
    else:
        raise NotImplementedError(
            "Live execution not enabled. Complete Phase 5 live sign-off first."
        )

    # 7. Return order_id
    return order_id
