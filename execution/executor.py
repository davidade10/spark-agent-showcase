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
import time
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    HARD_RULES, TRADING_MODE, ENABLE_LIVE_SEND,
    PAPER_ACCOUNT_NAV, LIVE_ACCOUNT_NAV,
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
# PAPER_ACCOUNT_NAV and LIVE_ACCOUNT_NAV are imported from config — see there for comments.

# ── Live routing helpers ──────────────────────────────────────────────────────
def _resolve_account_hash(client, account_id: str) -> str:
    """
    Map a human account_id (last-4 digits, e.g. '8096') to Schwab's internal
    accountHash (hashValue) via get_account_numbers().
    Raises ValueError if not found.
    """
    resp = client.get_account_numbers()
    resp.raise_for_status()
    entries = resp.json() or []
    for e in entries:
        acct_num = str(e.get("accountNumber") or "")
        hash_val = str(e.get("hashValue") or "")
        last4 = acct_num[-4:] if len(acct_num) >= 4 else acct_num
        if last4 == str(account_id):
            if not hash_val:
                break
            return hash_val
    raise ValueError(f"Could not resolve accountHash for account_id={account_id!r}")


# ── Safety check — always visible in logs ────────────────────────────────────
if TRADING_MODE == "live":
    logger.warning(
        "TRADING_MODE=live — real orders will be sent to Schwab. "
        "Complete Phase 5 live sign-off before enabling live execution."
    )
    if not ENABLE_LIVE_SEND:
        logger.warning(
            "ENABLE_LIVE_SEND is FALSE — live execution is in NO-SEND safe-mode. "
            "Orders will be payload-ready but will NOT be submitted."
        )

# ── Agent-config reader (executor-local, no cross-module import) ──────────────
def _read_live_account_flag(account_id: str) -> str:
    """
    Read live_trading_enabled_{account_id} from agent_config.

    Returns the raw string value on success, or 'false' on any failure
    (missing key, DB error).  Fail-closed by design.

    Uses executor's own DB_URL — does not import from rules_gate or api.py.
    """
    key = f"live_trading_enabled_{account_id}"
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT value FROM agent_config WHERE key = :key"),
                {"key": key},
            ).fetchone()
        if row is None:
            logger.warning(
                "_read_live_account_flag: key %r not found in agent_config — "
                "defaulting to 'false' (fail-closed)",
                key,
            )
            return "false"
        return str(row[0]).lower().strip()
    except Exception as exc:
        logger.warning(
            "_read_live_account_flag: DB error reading %r — "
            "defaulting to 'false' (fail-closed). Error: %s",
            key, exc,
        )
        return "false"


# ── Live execution guard (extracted for testability) ──────────────────────────
def _live_execution_guard(
    trading_mode: str,
    enable_live_send: bool,
    account_id: str,
) -> None:
    """
    Hard-fail if any live execution precondition is not met.

    Checks all three conditions independently and raises RuntimeError with
    the exact failure reason.  Called at the top of the live-send branch in
    execute_approved_candidate() — defense-in-depth against direct calls that
    bypass upstream guards (dashboard approval, autonomous cron, API hit).

    :param trading_mode:    current TRADING_MODE value
    :param enable_live_send: current ENABLE_LIVE_SEND value
    :param account_id:      4-digit account suffix (e.g. '8096')
    """
    if trading_mode != "live":
        reason = f"TRADING_MODE={trading_mode!r} — must be 'live' to send orders"
        logger.error("LIVE EXECUTION BLOCKED: %s", reason)
        raise RuntimeError(reason)

    if not enable_live_send:
        reason = "ENABLE_LIVE_SEND=false — live send is disabled in config"
        logger.error("LIVE EXECUTION BLOCKED: %s", reason)
        raise RuntimeError(reason)

    account_flag = _read_live_account_flag(account_id)
    if account_flag != "true":
        reason = (
            f"live_trading_enabled_{account_id}={account_flag!r} — "
            f"account {account_id} is not enabled for live trading"
        )
        logger.error("LIVE EXECUTION BLOCKED: %s", reason)
        raise RuntimeError(reason)


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


# ── Live NAV fetch ────────────────────────────────────────────────────────────
def _fetch_live_nav(account_id: str) -> float:
    """
    Fetch current NAV for account_id from GET /accounts (port 8000).

    Matching rules:
      - 'PAPER'       → matches account_id == 'PAPER'
      - last-4 digits → matches account_id string (e.g. '8096', '5760')

    On any failure (connection error, account not found, null NAV) falls back
    to PAPER_ACCOUNT_NAV or LIVE_ACCOUNT_NAV from config. Timeout is 3 seconds
    so a slow API call never blocks order execution.
    """
    fallback = PAPER_ACCOUNT_NAV if account_id == "PAPER" else LIVE_ACCOUNT_NAV
    try:
        import requests  # noqa: PLC0415 — lazy import; requests may not be installed
        resp = requests.get("http://localhost:8000/accounts", timeout=3)
        resp.raise_for_status()
        accounts = (resp.json() or {}).get("accounts") or []
        for acct in accounts:
            if str(acct.get("account_id")) == str(account_id):
                nav = acct.get("nav")
                if nav is not None:
                    logger.info(
                        "_fetch_live_nav: account_id=%r live NAV=%.2f", account_id, float(nav)
                    )
                    return float(nav)
        logger.warning(
            "_fetch_live_nav: account_id=%r not found in /accounts — using fallback %.0f",
            account_id, fallback,
        )
    except Exception as exc:
        logger.warning(
            "_fetch_live_nav: fetch failed for account_id=%r (%s) — using fallback %.0f",
            account_id, exc, fallback,
        )
    return fallback


# ── Quantity calculator ───────────────────────────────────────────────────────
def _compute_quantity(candidate_json: dict, account_id: str = "") -> int:
    """
    Quantity sizing via 6% max position risk rule:

        spread_width          = candidate_json['spread_width']  (already computed)
        max_loss_per_contract = (spread_width - net_credit) * 100
        max_risk_dollars      = NAV * max_position_risk_pct  (0.06)
        quantity              = floor(max_risk_dollars / max_loss_per_contract)
        minimum               = 1

    When account_id is provided, fetches live NAV via _fetch_live_nav() with
    automatic fallback to PAPER_ACCOUNT_NAV / LIVE_ACCOUNT_NAV from config.
    """
    if account_id:
        nav = _fetch_live_nav(account_id)
    else:
        nav = PAPER_ACCOUNT_NAV if TRADING_MODE != "live" else LIVE_ACCOUNT_NAV
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

        # 4. Compute quantity — live NAV fetched from /accounts; falls back to config values
        quantity = _compute_quantity(candidate_json, account_id)

        logger.info(
            f"execute_approved_candidate: id={candidate_id} symbol={symbol} "
            f"qty={quantity} mode={TRADING_MODE} account={account_id}"
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
        # Live safe-mode path: generate Schwab-ready payload + resolve account routing,
        # but do not submit unless ENABLE_LIVE_SEND is explicitly enabled.
        from data_layer.provider import get_schwab_client  # noqa: PLC0415

        client = get_schwab_client()
        account_hash = _resolve_account_hash(client, str(account_id))

        # Intended Schwab endpoint (informational; no POST performed unless kill switch enabled)
        intended_url = f"/accounts/{account_hash}/orders"

        if not ENABLE_LIVE_SEND:
            live_dry_id = f"LIVE-DRY-{int(time.time())}"
            payload_shadow = dict(payload)
            payload_shadow["live_dry_run_id"] = live_dry_id
            payload_shadow["intended_url"] = intended_url

            logger.info(f"[LIVE-DRY-RUN-PAYLOAD] candidate_id={candidate_id} order_id={order_id} account_id={account_id}")
            logger.info(f"[LIVE-DRY-RUN-PAYLOAD] intended_url={intended_url}")
            logger.info(f"[LIVE-DRY-RUN-PAYLOAD] payload={json.dumps(payload_shadow)}")

            # Persist that this was a no-send live dry run by updating the orders row.
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE orders
                    SET source = 'live_dry_run',
                        order_payload = cast(:payload as jsonb)
                    WHERE id = :id
                """), {"id": order_id, "payload": json.dumps(payload_shadow)})

            logger.info(
                f"Live NO-SEND routing complete — order_id={order_id} live_dry_run_id={live_dry_id} "
                f"account_id={account_id}"
            )
        else:
            account_suffix = str(account_id)

            # ── Live execution guard — last line of defense before Schwab ────
            # Raises RuntimeError with exact reason if any condition fails.
            # Defense-in-depth: checks duplicate upstream guards intentionally.
            _live_execution_guard(TRADING_MODE, ENABLE_LIVE_SEND, account_suffix)

            logger.info(
                "Live execution guard passed — TRADING_MODE=live ENABLE_LIVE_SEND=true "
                "live_trading_enabled_%s=true order_id=%s account=%s",
                account_suffix, order_id, account_suffix,
            )

            # ── Submit live order to Schwab ───────────────────────────────────
            import schwab.utils as schwab_utils  # noqa: PLC0415

            logger.info(
                "Submitting live order to Schwab — order_id=%s symbol=%s "
                "account=%s quantity=%s",
                order_id, symbol, account_suffix, quantity,
            )
            try:
                resp = client.place_order(account_hash, payload)
                resp.raise_for_status()
                schwab_order_id = schwab_utils.Utils(
                    account_hash, client
                ).extract_order_id(resp)
                logger.info(
                    "Live order accepted by Schwab — order_id=%s "
                    "schwab_order_id=%s account=%s symbol=%s",
                    order_id, schwab_order_id, account_suffix, symbol,
                )
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE orders
                        SET source          = 'live',
                            status          = 'submitted',
                            schwab_order_id = :schwab_order_id
                        WHERE id = :id
                    """), {
                        "id":              order_id,
                        "schwab_order_id": str(schwab_order_id) if schwab_order_id is not None else None,
                    })
            except Exception as exc:
                logger.error(
                    "Live order submission FAILED — order_id=%s symbol=%s "
                    "account=%s error=%s",
                    order_id, symbol, account_suffix, exc,
                )
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE orders
                            SET status        = 'failed',
                                error_message = :err
                            WHERE id = :id
                        """), {"id": order_id, "err": str(exc)})
                except Exception as db_exc:
                    logger.error(
                        "Could not write failed status — order_id=%s db_error=%s",
                        order_id, db_exc,
                    )
                raise

    # 7. Return order_id
    return order_id
