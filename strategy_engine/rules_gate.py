"""
rules_gate.py — Strategy Engine
Pure deterministic hard rules filter. No LLM, no scoring, no judgment.

Every candidate from scoring.py passes through this gate before reaching
the LLM or approval UI. A candidate fails the moment it fails any single rule.

Rules enforced (in order of check):
  1. Max open condors          — no more than 4 open at once
  2. Short delta               — both short strikes must be ≤ 0.22 delta
  3. Net credit                — must collect at least $0.40
  4. Position risk             — max loss must be ≤ 1% of account NAV
  5. Correlated risk           — combined risk on correlated underlyings ≤ 3% NAV
  6. Earnings proximity        — no new positions within 5 days of earnings
  7. FOMC proximity            — no new positions within 1 days of FOMC
  8. Underlying volume         — underlying ADV must be ≥ 1,000,000 shares
  9. Daily loss kill switch    — block all new positions if account down ≥ 3% today
 10. Open interest             — both short strikes must have OI ≥ 100; block if unverifiable

Candidates that pass all rules → gate_result = "approved"
Candidates that fail any rule → gate_result = "blocked" + blocking_rule recorded

Both outcomes are written to trade_candidates table for full audit trail.
The LLM layer only ever sees approved candidates.
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

from sqlalchemy import create_engine, text

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, HARD_RULES, TRADING_MODE
from data_layer.events_calendar import is_earnings_within_days, is_fomc_within_days
from data_layer.notifier import send_telegram_msg
from strategy_engine.candidates import IronCondorCandidate, StrangleCandidate
from strategy_engine.scoring import ScoredCandidate

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Symbols considered correlated for risk aggregation purposes.
# If you hold IWM and SPY simultaneously, their risks are additive
# because they move together. This map groups them.
CORRELATION_GROUPS: dict[str, str] = {
    "SPY":  "us_large_cap",
    "QQQ":  "us_large_cap",
    "IWM":  "us_small_cap",
    "NVDA": "semis",
    "AAPL": "us_large_cap",
}


# ── Gate result dataclass ─────────────────────────────────────────────────────
@dataclass
class GateResult:
    """
    Result of running a ScoredCandidate through the rules gate.

    passed:        True if all rules cleared, False if any rule blocked
    gate_result:   "approved" or "blocked"
    blocking_rule: name of the first rule that failed (None if approved)
    blocking_reason: human-readable explanation of the failure
    candidate_id:  database ID of the trade_candidates row written
    """
    passed:           bool
    gate_result:      str             # "approved" | "blocked"
    blocking_rule:    Optional[str]
    blocking_reason:  Optional[str]
    candidate_id:     Optional[int]
    scored:           ScoredCandidate


# ── Account context ───────────────────────────────────────────────────────────
@dataclass
class AccountContext:
    """
    Snapshot of account state needed for position-based rules.
    Fetched once per gate run from Schwab and passed to all checks.

    nav:               current liquidation value of account
    open_condors:      number of currently open iron condor positions
    daily_pnl_pct:     today's P&L as a percentage of starting NAV
                       (negative means loss, e.g. -0.025 = down 2.5%)
    open_symbols:      set of symbols with currently open condors
    correlated_risk:   dict mapping correlation_group → total max loss open
    """
    nav:              float
    open_condors:     int
    daily_pnl_pct:    float
    open_symbols:     set
    correlated_risk:  dict


def get_account_contexts(client) -> list[tuple[str, AccountContext]]:
    """
    Fetches account state for ALL accounts from Schwab.
    Returns a list of (account_label, AccountContext) tuples —
    one per account.
    """
    try:
        response = client.get_accounts(fields=client.Account.Fields.POSITIONS)
        accounts = response.json()

        if not accounts:
            raise ValueError("No accounts returned from Schwab")

        result = []

        for acc in accounts:
            acct    = acc.get("securitiesAccount", {})
            acct_id = str(acct.get("accountNumber", "unknown"))
            label   = f"...{acct_id[-4:]}"

            nav = float(
                acct.get("currentBalances", {})
                    .get("liquidationValue", 0)
            )

            positions = acct.get("positions", [])

            open_condors:    int             = 0
            open_symbols:    set             = set()
            correlated_risk: dict            = {}
            symbol_short_counts: dict[str, int] = {}

            # Count iron condors only — non-condor positions (covered calls,
            # naked puts, verticals, equity from assignment) are tracked in
            # the DB for risk visibility but do NOT count toward the condor
            # limit.  Iron condors always have exactly 2 short option legs
            # per underlying+expiry; count // 2 naturally excludes single-leg
            # non-condor positions (1 // 2 == 0) and strangle pairs that
            # aren't iron condors.
            for pos in positions:
                instrument = pos.get("instrument", {})
                if instrument.get("assetType", "") != "OPTION":
                    continue
                underlying = instrument.get("underlyingSymbol", "")
                short_qty  = pos.get("shortQuantity", 0)
                if short_qty > 0:
                    symbol_short_counts[underlying] = (
                        symbol_short_counts.get(underlying, 0) + 1
                    )

            for symbol, count in symbol_short_counts.items():
                # count // 2: requires 2 short legs (put + call) to form a
                # condor.  Covered calls (1 short call) and naked puts
                # (1 short put) give count=1 → 0 condors counted.
                condors = count // 2
                if condors > 0:
                    open_condors += condors
                    open_symbols.add(symbol)
                    group = CORRELATION_GROUPS.get(symbol, symbol)
                    correlated_risk[group] = correlated_risk.get(group, 0)

            ctx = AccountContext(
                nav             = nav,
                open_condors    = open_condors,
                daily_pnl_pct   = 0.0,
                open_symbols    = open_symbols,
                correlated_risk = correlated_risk,
            )

            result.append((label, ctx))
            logger.info(
                f"Account {label}: NAV=${nav:,.2f} | "
                f"Open condors: {open_condors}"
            )

        return result

    except Exception as e:
        logger.error(f"Failed to fetch account contexts — {e}")
        return [("fallback", AccountContext(
            nav             = 0.0,
            open_condors    = HARD_RULES["max_open_condors_live"],
            daily_pnl_pct   = 0.0,
            open_symbols    = set(),
            correlated_risk = {},
        ))]


def get_underlying_volume(client, symbol: str) -> Optional[float]:
    """
    Fetches the average daily volume for a symbol from Schwab fundamentals.
    Returns None if the API call fails — volume rule will block on None.
    """
    try:
        response = client.get_instruments(
            symbols    = [symbol],
            projection = client.Instrument.Projection.FUNDAMENTAL,
        )
        data = response.json()

        instruments = data.get("instruments") or []
        if not instruments:
            return None

        fundamental = instruments[0].get("fundamental", {}) or {}
        return fundamental.get("vol1DayAvg") or fundamental.get("averageVolume")

    except Exception as e:
        logger.warning(f"{symbol}: volume fetch failed — {e}")
        return None


# ── Individual rule checks ────────────────────────────────────────────────────
def _check_max_open_condors(conn, account_id: str) -> Optional[str]:
    """
    Counts open IRON_CONDOR positions for this specific account in the DB.

    PAPER accounts use max_open_condors_paper; any live account uses
    max_open_condors_live.  Blocks when count >= limit (not strictly >).

    Queries the positions table directly so PAPER positions (not reflected in
    Schwab's API) are counted correctly and live/paper limits are independent.
    """
    is_paper = (account_id == "PAPER")
    limit = (
        HARD_RULES["max_open_condors_paper"]
        if is_paper
        else HARD_RULES["max_open_condors_live"]
    )

    row = conn.execute(text("""
        SELECT COUNT(*) AS cnt
        FROM positions
        WHERE status     = 'open'
          AND account_id = :account_id
          AND strategy   = 'IRON_CONDOR'
    """), {"account_id": account_id}).fetchone()

    open_condors = int(row.cnt) if row else 0

    if open_condors >= limit:
        acct_label = "PAPER account" if is_paper else f"account {account_id}"
        return (
            f"{acct_label} has {open_condors} open condors (limit {limit})"
        )
    return None


def _is_live_account_enabled(engine, gate_account_id: str) -> bool:
    """
    Returns True if live trading is enabled for this account.

    Reads live_trading_enabled_{gate_account_id} from agent_config.
    Fail-closed: missing key, non-'true' value, or DB error → False.

    Only called for non-PAPER accounts. PAPER is always allowed through
    this check (controlled separately by TRADING_MODE).
    """
    key = f"live_trading_enabled_{gate_account_id}"
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT value FROM agent_config WHERE key = :key"
            ), {"key": key}).fetchone()
        if row is None:
            logger.info(
                f"_is_live_account_enabled: key '{key}' not found in agent_config — "
                f"treating account {gate_account_id} as disabled"
            )
            return False
        enabled = str(row[0]).lower().strip() in ("true", "1", "yes")
        if not enabled:
            logger.info(
                f"_is_live_account_enabled: '{key}' = {row[0]!r} — "
                f"account {gate_account_id} is not enabled for live trading"
            )
        return enabled
    except Exception as e:
        logger.warning(
            f"_is_live_account_enabled: DB error reading '{key}' — "
            f"treating account {gate_account_id} as disabled. Error: {e}"
        )
        return False


def _check_strangle_trading_enabled(conn) -> Optional[str]:
    """
    Reads strangle_trading_enabled from agent_config.
    Returns a block reason if the value is not 'true'; None if enabled.
    Fail-closed: any DB error or missing key → blocked.
    """
    try:
        row = conn.execute(text("""
            SELECT value FROM agent_config WHERE key = 'strangle_trading_enabled'
        """)).fetchone()
        if row is None:
            return "Strangle trading is disabled (config key not found — defaulting to disabled)"
        if str(row[0]).lower().strip() not in ("true", "1", "yes"):
            return "Strangle trading is disabled — set strangle_trading_enabled=true to enable"
    except Exception as e:
        logger.warning(f"_check_strangle_trading_enabled: DB error — {e}")
        return "Strangle trading check failed — defaulting to disabled"
    return None


def _check_max_open_strangles(conn, account_id: str) -> Optional[str]:
    """
    Counts open STRANGLE positions for this account.
    Limits are separate from the condor limit (strangles carry unlimited risk).
    Uses HARD_RULES keys max_open_strangles_paper / max_open_strangles_live
    with fallbacks of 4 (paper) and 2 (live).
    """
    is_paper = (account_id == "PAPER")
    limit = (
        HARD_RULES.get("max_open_strangles_paper", 4)
        if is_paper
        else HARD_RULES.get("max_open_strangles_live", 2)
    )

    row = conn.execute(text("""
        SELECT COUNT(*) AS cnt
        FROM positions
        WHERE status     = 'open'
          AND account_id = :account_id
          AND strategy   = 'STRANGLE'
    """), {"account_id": account_id}).fetchone()

    open_strangles = int(row.cnt) if row else 0

    if open_strangles >= limit:
        acct_label = "PAPER account" if is_paper else f"account {account_id}"
        return (
            f"{acct_label} has {open_strangles} open strangles (limit {limit})"
        )
    return None


def _check_short_delta(scored: ScoredCandidate) -> Optional[str]:
    c = scored.candidate
    put_delta  = abs(c.short_put_delta)
    call_delta = abs(c.short_call_delta)

    if put_delta > HARD_RULES["max_short_delta"]:
        return (
            f"Short put delta {put_delta:.3f} exceeds "
            f"max {HARD_RULES['max_short_delta']}"
        )
    if call_delta > HARD_RULES["max_short_delta"]:
        return (
            f"Short call delta {call_delta:.3f} exceeds "
            f"max {HARD_RULES['max_short_delta']}"
        )
    return None


def _check_net_credit(scored: ScoredCandidate) -> Optional[str]:
    if scored.candidate.net_credit < HARD_RULES["min_net_credit"]:
        return (
            f"Net credit ${scored.candidate.net_credit:.2f} below "
            f"minimum ${HARD_RULES['min_net_credit']:.2f}"
        )
    return None


def _check_position_risk(
    scored: ScoredCandidate,
    ctx:    AccountContext,
) -> Optional[str]:
    """
    Checks that max loss on this trade doesn't exceed 6% of NAV.  <-- CHANGE THIS
    Max loss per contract = spread_width - net_credit (per share × 100).
    We assume 1 contract (100 shares) as the default position size.
    """
    if ctx.nav <= 0:
        return "Cannot verify position risk — NAV unavailable"

    max_loss = getattr(scored.candidate, "max_loss", None)
    if max_loss is None:
        logger.warning(
            f"{scored.candidate.symbol}: max_loss undefined "
            f"(strategy={getattr(scored.candidate, 'strategy', 'unknown')}) "
            f"— skipping position_risk check"
        )
        return None

    max_loss_dollars = max_loss * 100  # per contract
    risk_pct         = max_loss_dollars / ctx.nav

    # This logic is already correct because it pulls 0.06 from config.py
    if risk_pct > HARD_RULES["max_position_risk_pct"]:
        return (
            f"Position risk {risk_pct:.2%} exceeds "
            f"max {HARD_RULES['max_position_risk_pct']:.2%} of NAV "
            f"(max loss ${max_loss_dollars:.0f} on NAV ${ctx.nav:,.0f})"
        )
    return None

def _check_correlated_risk(
    scored: ScoredCandidate,
    ctx:    AccountContext,
) -> Optional[str]:
    """
    Checks that combined risk in the same correlation group
    doesn't exceed 3% of NAV after adding this trade.
    """
    if ctx.nav <= 0:
        return "Cannot verify correlated risk — NAV unavailable"

    symbol = scored.candidate.symbol
    group  = CORRELATION_GROUPS.get(symbol, symbol)

    max_loss = getattr(scored.candidate, "max_loss", None)
    if max_loss is None:
        logger.warning(
            f"{scored.candidate.symbol}: max_loss undefined "
            f"— skipping correlated_risk check"
        )
        return None

    existing_risk    = ctx.correlated_risk.get(group, 0.0)
    new_risk_dollars = max_loss * 100
    total_risk       = existing_risk + new_risk_dollars
    total_risk_pct   = total_risk / ctx.nav

    if total_risk_pct > HARD_RULES["max_correlated_risk_pct"]:
        return (
            f"Correlated risk in group '{group}' would be "
            f"{total_risk_pct:.2%} (max {HARD_RULES['max_correlated_risk_pct']:.2%} of NAV)"
        )
    return None


def _check_earnings_proximity(scored: ScoredCandidate) -> Optional[str]:
    symbol = scored.candidate.symbol
    days   = HARD_RULES["blocked_within_earnings_days"]

    try:
        if is_earnings_within_days(symbol, days):
            return (
                f"{symbol} has earnings within {days} days — "
                f"position blocked until after earnings"
            )
    except Exception as e:
        logger.warning(f"{symbol}: earnings check failed — {e}")
        # Don't block on check failure — earnings data may just not exist yet

    return None


def _check_fomc_proximity() -> Optional[str]:
    days = HARD_RULES["blocked_within_fomc_days"]

    try:
        if is_fomc_within_days(days):
            return (
                f"FOMC meeting within {days} days — "
                f"no new positions until after meeting"
            )
    except Exception as e:
        logger.warning(f"FOMC check failed — {e}")

    return None


def _check_underlying_volume(
    scored:  ScoredCandidate,
    client,
) -> Optional[str]:
    symbol = scored.candidate.symbol
    volume = get_underlying_volume(client, symbol)

    if volume is None:
        logger.warning(f"{symbol}: could not verify volume — skipping volume check")
        return None  # Don't block if we can't verify — log and continue

    if volume < HARD_RULES["min_underlying_adv"]:
        return (
            f"{symbol} ADV {volume:,.0f} below "
            f"minimum {HARD_RULES['min_underlying_adv']:,.0f}"
        )
    return None


def _get_short_strike_oi(
    conn,
    scored: ScoredCandidate,
) -> tuple[Optional[int], Optional[int]]:
    """
    Looks up open interest for the short put and short call strikes
    from option_quotes. Returns (put_oi, call_oi); either value is None
    if the row cannot be found or OI is NULL.
    """
    c = scored.candidate

    row_put = conn.execute(text("""
        SELECT open_interest FROM option_quotes
        WHERE symbol      = :symbol
          AND snapshot_id = :snapshot_id
          AND expiry      = :expiry
          AND strike      = :strike
          AND option_right = 'P'
        LIMIT 1
    """), {
        "symbol":      c.symbol,
        "snapshot_id": c.snapshot_id,
        "expiry":      c.expiry,
        "strike":      c.short_put_strike,
    }).fetchone()

    row_call = conn.execute(text("""
        SELECT open_interest FROM option_quotes
        WHERE symbol      = :symbol
          AND snapshot_id = :snapshot_id
          AND expiry      = :expiry
          AND strike      = :strike
          AND option_right = 'C'
        LIMIT 1
    """), {
        "symbol":      c.symbol,
        "snapshot_id": c.snapshot_id,
        "expiry":      c.expiry,
        "strike":      c.short_call_strike,
    }).fetchone()

    put_oi  = int(row_put.open_interest)  if row_put  and row_put.open_interest  is not None else None
    call_oi = int(row_call.open_interest) if row_call and row_call.open_interest is not None else None

    return put_oi, call_oi


def _check_open_interest(scored: ScoredCandidate, conn) -> Optional[str]:
    """
    Both short strikes must have open_interest >= min_short_strike_oi.
    Blocks (does not skip) if OI data cannot be found — illiquid strikes
    are a real risk and an unverifiable OI should not pass silently.
    """
    min_oi = HARD_RULES["min_short_strike_oi"]
    c      = scored.candidate

    put_oi, call_oi = _get_short_strike_oi(conn, scored)

    if put_oi is None:
        return (
            f"Cannot verify OI for short put "
            f"${c.short_put_strike:.0f}P {c.expiry} — blocking"
        )
    if call_oi is None:
        return (
            f"Cannot verify OI for short call "
            f"${c.short_call_strike:.0f}C {c.expiry} — blocking"
        )
    if put_oi < min_oi:
        return (
            f"Short put ${c.short_put_strike:.0f}P OI={put_oi} "
            f"below minimum {min_oi}"
        )
    if call_oi < min_oi:
        return (
            f"Short call ${c.short_call_strike:.0f}C OI={call_oi} "
            f"below minimum {min_oi}"
        )
    return None


def _check_daily_loss_kill(ctx: AccountContext) -> Optional[str]:
    if ctx.daily_pnl_pct <= -HARD_RULES["daily_loss_kill_pct"]:
        return (
            f"Daily loss kill switch active — account down "
            f"{abs(ctx.daily_pnl_pct):.2%} "
            f"(threshold {HARD_RULES['daily_loss_kill_pct']:.2%})"
        )
    return None


def _blocked_numeric_extras(
    rule_name:        str,
    scored:           ScoredCandidate,
    ctx:              AccountContext,
    conn,
    gate_account_id:  str,
    client,
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Optional (actual, threshold, operator) for blocked_reason JSONB.
    operator describes the pass condition; failure means actual violates it.
    Boolean / proximity rules return (None, None, None).
    """
    c = scored.candidate

    if rule_name == "net_credit":
        return (
            float(c.net_credit),
            float(HARD_RULES["min_net_credit"]),
            ">=",
        )

    if rule_name == "short_delta":
        put_d = abs(c.short_put_delta)
        call_d = abs(c.short_call_delta)
        th = float(HARD_RULES["max_short_delta"])
        if put_d > th:
            return (put_d, th, "<=")
        if call_d > th:
            return (call_d, th, "<=")
        return (None, None, None)

    if rule_name == "position_risk":
        if ctx.nav <= 0:
            return (None, None, None)
        _max_loss = getattr(c, "max_loss", None)
        if _max_loss is None:
            return (None, None, None)
        max_loss_d = _max_loss * 100
        risk_pct = max_loss_d / ctx.nav
        return (risk_pct, float(HARD_RULES["max_position_risk_pct"]), "<=")

    if rule_name == "correlated_risk":
        if ctx.nav <= 0:
            return (None, None, None)
        _max_loss = getattr(c, "max_loss", None)
        if _max_loss is None:
            return (None, None, None)
        symbol = c.symbol
        group = CORRELATION_GROUPS.get(symbol, symbol)
        existing = ctx.correlated_risk.get(group, 0.0)
        new_risk = _max_loss * 100
        total_risk = existing + new_risk
        total_risk_pct = total_risk / ctx.nav
        return (total_risk_pct, float(HARD_RULES["max_correlated_risk_pct"]), "<=")

    if rule_name == "underlying_volume":
        volume = get_underlying_volume(client, c.symbol)
        if volume is None:
            return (None, None, None)
        return (float(volume), float(HARD_RULES["min_underlying_adv"]), ">=")

    if rule_name == "max_open_condors":
        is_paper = gate_account_id == "PAPER"
        limit = (
            HARD_RULES["max_open_condors_paper"]
            if is_paper
            else HARD_RULES["max_open_condors_live"]
        )
        row = conn.execute(text("""
            SELECT COUNT(*) AS cnt
            FROM positions
            WHERE status     = 'open'
              AND account_id = :account_id
              AND strategy   = 'IRON_CONDOR'
        """), {"account_id": gate_account_id}).fetchone()
        cnt = int(row.cnt) if row else 0
        return (float(cnt), float(limit), ">=")

    if rule_name == "open_interest":
        min_oi = float(HARD_RULES["min_short_strike_oi"])
        put_oi, call_oi = _get_short_strike_oi(conn, scored)
        if put_oi is not None and put_oi < min_oi:
            return (float(put_oi), min_oi, ">=")
        if call_oi is not None and call_oi < min_oi:
            return (float(call_oi), min_oi, ">=")
        return (None, None, None)

    # earnings_proximity, fomc_proximity, daily_loss_kill, rule errors → boolean / non-numeric
    return (None, None, None)


def _resolve_qty_for_candidate_json(c: Any) -> Optional[int]:
    """
    Integer qty (contracts per leg) for candidate_json, or None if unknown.

    IronCondorCandidate always persists a positive int: field default is 1, and
    pre-qty legacy instances (attr missing on instance) fall back to 1.
    Other objects use qty / contracts / position_size only when set.
    """
    if isinstance(c, IronCondorCandidate):
        try:
            v = int(getattr(c, "qty", 1))
        except (TypeError, ValueError):
            v = 1
        return v if v > 0 else 1

    for attr in ("qty", "contracts", "position_size"):
        raw = getattr(c, attr, None)
        if raw is None:
            continue
        try:
            qf = float(raw)
            if not math.isnan(qf) and not math.isinf(qf) and qf >= 0:
                return int(round(qf))
        except (TypeError, ValueError):
            continue
    return None


# ── Database writer ───────────────────────────────────────────────────────────
def _write_to_trade_candidates(
    conn,
    scored:          ScoredCandidate,
    gate_result:     str,
    blocking_rule:   Optional[str],
    blocking_reason: Optional[str],
    account_id:      str = "PAPER",
    block_actual:    Optional[float] = None,
    block_threshold: Optional[float] = None,
    block_operator:  Optional[str] = None,
) -> int:
    """
    Writes the candidate and gate result to trade_candidates table.
    Returns the database ID of the inserted row.

    Written regardless of pass/fail — the blocked candidates are just as
    important for the audit trail as the approved ones.
    """
    c = scored.candidate

    _qty_out = _resolve_qty_for_candidate_json(c)

    candidate_json = {
        "symbol":             c.symbol,
        "expiry":             c.expiry,
        "dte":                c.dte,
        "short_put_strike":   c.short_put_strike,
        "short_call_strike":  c.short_call_strike,
        "net_credit":         c.net_credit,
        "underlying_price":   c.underlying_price,
        "iv_rank":            c.iv_rank,
        "short_put_delta":    c.short_put_delta,
        "short_call_delta":   c.short_call_delta,
        # Condor-specific (None for strangles):
        "long_put_strike":    getattr(c, "long_put_strike",  None),
        "long_call_strike":   getattr(c, "long_call_strike", None),
        "spread_width":       getattr(c, "spread_width",     None),
        "max_loss":           getattr(c, "max_loss",         None),
        # Strangle-specific (None for condors):
        "short_put_credit":   getattr(c, "short_put_credit",  None),
        "short_call_credit":  getattr(c, "short_call_credit", None),
    }
    if _qty_out is not None:
        candidate_json["qty"] = _qty_out

    logger.debug(
        "trade_candidates_write sym=%s gate=%s acct=%s scored_t=%s cand_t=%s "
        "qty_attr=%r contracts=%r pos_sz=%r resolved=%r json_has_qty=%s",
        c.symbol,
        gate_result,
        account_id,
        type(scored).__name__,
        type(c).__name__,
        getattr(c, "qty", None),
        getattr(c, "contracts", None),
        getattr(c, "position_size", None),
        _qty_out,
        "qty" in candidate_json,
    )

    score_json = {
        "total_score":        scored.total_score,
        "iv_rank_score":      scored.iv_rank_score,
        "credit_width_score": scored.credit_width_score,
        "delta_score":        scored.delta_score,
        "dte_score":          scored.dte_score,
        "call_delta_score":   scored.call_delta_score,
        "score_notes":        scored.score_notes,
        "blocking_rule":      blocking_rule,
        "blocking_reason":    blocking_reason,
    }

    # Construct blocked_reason JSONB — populated only when gate_result='blocked'.
    # Kept as a dedicated column so GET /shadow can query it directly without
    # parsing llm_card. None is stored as SQL NULL for approved candidates.
    blocked_reason_json: Optional[str] = None
    if blocking_rule:
        payload: dict[str, Any] = {
            "rule":   blocking_rule,
            "detail": blocking_reason or "",
        }
        if block_actual is not None or block_threshold is not None or block_operator:
            payload["actual"] = block_actual
            payload["threshold"] = block_threshold
            payload["operator"] = block_operator
        blocked_reason_json = json.dumps(payload)

    _strategy_col = getattr(c, "strategy", "iron_condor").lower()

    result = conn.execute(text("""
        INSERT INTO trade_candidates (
            created_at,
            snapshot_id,
            symbol,
            strategy,
            score,
            candidate_json,
            llm_card,
            gate_result,
            account_id,
            blocked_reason
        ) VALUES (
            :created_at,
            :snapshot_id,
            :symbol,
            :strategy,
            :score,
            :candidate_json,
            :score_json,
            :gate_result,
            :account_id,
            cast(:blocked_reason as jsonb)
        )
        RETURNING id
    """), {
        "created_at":      datetime.now(timezone.utc),
        "snapshot_id":     c.snapshot_id,
        "symbol":          c.symbol,
        "strategy":        _strategy_col,
        "score":           scored.total_score,
        "candidate_json":  json.dumps(candidate_json),
        "score_json":      json.dumps(score_json),
        "gate_result":     gate_result,
        "account_id":      account_id,
        "blocked_reason":  blocked_reason_json,
    })

    conn.commit()
    return result.scalar()


# ── Main gate runner ──────────────────────────────────────────────────────────
def run_gate(
    scored_candidates: list[ScoredCandidate],
    client,
) -> dict[str, list[GateResult]]:
    """
    Runs the gate separately for each Schwab account.
    Returns a dict of {account_label: [GateResult, ...]}
    so you can see which candidates are approved per account.
    """
    if not scored_candidates:
        logger.warning("run_gate called with empty candidate list")
        return {}

    engine           = create_engine(DB_URL)
    all_results: dict[str, list[GateResult]] = {}

    # Retire old visible pending before inserting new batch (prevents stale cards in UI)
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE trade_candidates
            SET llm_card = jsonb_set(
                COALESCE(llm_card, '{}'::jsonb),
                '{approval_status}',
                '"stale"',
                true
            )
            WHERE gate_result = 'approved'
              AND (llm_card IS NOT NULL AND llm_card != '{}'::jsonb)
              AND (llm_card ? 'recommendation')
              AND COALESCE(llm_card->>'approval_status', '') NOT IN ('approved', 'working')
        """))
        retired = result.rowcount
    if retired:
        logger.info(f"Retired {retired} stale pending trade cards before inserting new batch")

    logger.info("Fetching account contexts from Schwab...")
    account_contexts = get_account_contexts(client)

    # In paper mode the only relevant account is PAPER.  Schwab's API only
    # returns real brokerage accounts; PAPER positions live exclusively in
    # the DB.  Replace the Schwab-derived list so the gate loop evaluates
    # PAPER once — with the correct NAV and condor limit — instead of
    # iterating live accounts with a hard-coded "PAPER" gate_account_id.
    if TRADING_MODE == "paper":
        paper_nav = float(os.getenv("PAPER_ACCOUNT_NAV", "20000"))
        account_contexts = [("PAPER", AccountContext(
            nav             = paper_nav,
            open_condors    = 0,
            daily_pnl_pct   = 0.0,
            open_symbols    = set(),
            correlated_risk = {},
        ))]
        logger.info(f"Paper mode: evaluating PAPER account only (NAV=${paper_nav:,.2f})")

    for account_label, ctx in account_contexts:
        # Derive the account_id used for DB queries and limit selection.
        #
        # TRADING_MODE='paper' takes priority: PAPER positions live only in the DB
        # (not in any Schwab account context), so we must check account_id='PAPER'.
        #
        # For live accounts, account_label is "...XXXX" (last-4 of account number).
        # Stripping leading dots yields the exact account_id stored in positions.
        #
        # "fallback" is the error label returned by get_account_contexts() when
        # Schwab auth fails; other gate checks (nav=0) block trades in that case.
        if TRADING_MODE == "paper":
            gate_account_id = "PAPER"
        elif account_label.startswith("..."):
            gate_account_id = account_label.lstrip(".")
        else:
            gate_account_id = account_label  # "fallback" or unexpected value

        # Per-account live enable check.
        # PAPER always proceeds — it is controlled by TRADING_MODE, not this flag.
        # For every live account, live_trading_enabled_{suffix} must be 'true'
        # in agent_config or the gate skips this account entirely.
        if gate_account_id != "PAPER" and not _is_live_account_enabled(engine, gate_account_id):
            logger.info(
                f"Gate: skipping account {gate_account_id} — "
                f"live_trading_enabled_{gate_account_id} is not set to 'true'"
            )
            continue

        logger.info(f"\n── Running gate for account {account_label} ──────────")
        results = []

        with engine.connect() as conn:
            for scored in scored_candidates:
                symbol          = scored.candidate.symbol
                blocking_rule   = None
                blocking_reason = None

                if isinstance(scored.candidate, StrangleCandidate):
                    # Strangle gate: no position_risk / correlated_risk (unlimited max loss).
                    # strangle_trading_disabled is checked first — if disabled, all strangles
                    # are blocked immediately and recorded in /shadow for monitoring.
                    checks = [
                        ("strangle_trading_disabled", lambda: _check_strangle_trading_enabled(conn)),
                        ("max_open_strangles", lambda: _check_max_open_strangles(conn, gate_account_id)),
                        ("daily_loss_kill",    lambda: _check_daily_loss_kill(ctx)),
                        ("net_credit",         lambda: _check_net_credit(scored)),
                        ("short_delta",        lambda: _check_short_delta(scored)),
                        ("fomc_proximity",     lambda: _check_fomc_proximity()),
                        ("earnings_proximity", lambda: _check_earnings_proximity(scored)),
                        ("underlying_volume",  lambda: _check_underlying_volume(scored, client)),
                        ("open_interest",      lambda: _check_open_interest(scored, conn)),
                    ]
                else:
                    checks = [
                        ("max_open_condors",   lambda: _check_max_open_condors(conn, gate_account_id)),
                        ("daily_loss_kill",    lambda: _check_daily_loss_kill(ctx)),
                        ("net_credit",         lambda: _check_net_credit(scored)),
                        ("short_delta",        lambda: _check_short_delta(scored)),
                        ("fomc_proximity",     lambda: _check_fomc_proximity()),
                        ("earnings_proximity", lambda: _check_earnings_proximity(scored)),
                        ("position_risk",      lambda: _check_position_risk(scored, ctx)),
                        ("correlated_risk",    lambda: _check_correlated_risk(scored, ctx)),
                        ("underlying_volume",  lambda: _check_underlying_volume(scored, client)),
                        ("open_interest",      lambda: _check_open_interest(scored, conn)),
                    ]

                for rule_name, check_fn in checks:
                    try:
                        failure_reason = check_fn()
                        if failure_reason:
                            blocking_rule   = rule_name
                            blocking_reason = failure_reason
                            break
                    except Exception as e:
                        logger.error(f"{symbol}: rule '{rule_name}' crashed — {e}")
                        blocking_rule   = rule_name
                        blocking_reason = f"Rule check error: {e}"
                        break

                passed      = blocking_rule is None
                gate_result = "approved" if passed else "blocked"

                if passed:
                    logger.info(
                        f"✓ APPROVED [{account_label}]: {symbol} "
                        f"{scored.candidate.expiry} "
                        f"score={scored.total_score:.1f} "
                        f"credit=${scored.candidate.net_credit:.2f}"
                    )
                else:
                    logger.info(
                        f"✗ BLOCKED  [{account_label}]: {symbol} "
                        f"{scored.candidate.expiry} "
                        f"— {blocking_rule}: {blocking_reason}"
                    )

                # Dedup: skip if this symbol/expiry/gate_result was already
                # written within the last 2 hours.
                #
                # Scope dedup by gate_result so an approved row from a prior
                # run never suppresses a blocked write (or vice versa), and
                # the 2-hour window prevents stale rows from days/weeks ago
                # from blocking fresh writes after conditions have changed.
                try:
                    existing = conn.execute(text("""
                        SELECT id FROM trade_candidates
                        WHERE symbol = :symbol
                          AND candidate_json->>'expiry' = :expiry
                          AND gate_result = :gate_result
                          AND account_id  = :account_id
                          AND created_at >= NOW() - INTERVAL '2 hours'
                        LIMIT 1
                    """), {
                        "symbol":      symbol,
                        "expiry":      scored.candidate.expiry,
                        "gate_result": gate_result,
                        "account_id":  gate_account_id,
                    }).fetchone()
                except Exception as e:
                    logger.warning(f"{symbol}: dedup check failed — {e}")
                    existing = None

                if existing:
                    logger.debug(
                        f"Skipping duplicate: {symbol} {scored.candidate.expiry} "
                        f"already in trade_candidates (id={existing[0]})"
                    )
                    candidate_id = existing[0]
                else:
                    try:
                        ba, bt, bo = (None, None, None)
                        if blocking_rule:
                            ba, bt, bo = _blocked_numeric_extras(
                                blocking_rule,
                                scored,
                                ctx,
                                conn,
                                gate_account_id,
                                client,
                            )
                        candidate_id = _write_to_trade_candidates(
                            conn,
                            scored,
                            gate_result,
                            blocking_rule,
                            blocking_reason,
                            account_id=gate_account_id,
                            block_actual=ba,
                            block_threshold=bt,
                            block_operator=bo,
                        )
                        if passed and candidate_id is not None:
                            try:
                                from datetime import date as _date
                                _c          = scored.candidate
                                _exp        = _date.fromisoformat(_c.expiry)
                                _expiry_fmt = f"{_exp.strftime('%b')} {_exp.day}"
                                _contracts  = (
                                    " · paper contracts"
                                    if gate_account_id == "PAPER"
                                    else ""
                                )
                                _msg = (
                                    f"🔥 *TRADE APPROVED* — {symbol} | {gate_account_id}\n"
                                    f"{_expiry_fmt}{_contracts}\n"
                                    f"${_c.long_put_strike:g}P / ${_c.short_put_strike:g}P"
                                    f" — ${_c.short_call_strike:g}C / ${_c.long_call_strike:g}C\n"
                                    f"Credit: ${_c.net_credit:.2f} | Score: {scored.total_score:.1f}\n"
                                    f"→ Check UI to execute"
                                )
                                send_telegram_msg(_msg)
                            except Exception as tg_exc:
                                logger.warning(
                                    f"{symbol}: approval Telegram notification failed — {tg_exc}"
                                )
                    except Exception as e:
                        logger.error(f"{symbol}: failed to write to trade_candidates — {e}")
                        candidate_id = None

                results.append(GateResult(
                    passed          = passed,
                    gate_result     = gate_result,
                    blocking_rule   = blocking_rule,
                    blocking_reason = blocking_reason,
                    candidate_id    = candidate_id,
                    scored          = scored,
                ))

        approved = sum(1 for r in results if r.passed)
        blocked  = sum(1 for r in results if not r.passed)
        logger.info(
            f"Account {account_label}: {approved} approved, "
            f"{blocked} blocked out of {len(results)}"
        )
        all_results[account_label] = results

    return all_results

# ── Manual test run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    from data_layer.provider        import get_schwab_client
    from strategy_engine.candidates import scan_for_candidates
    from strategy_engine.scoring    import score_candidates

    print("Running full pipeline: scan → score → gate...\n")

    client     = get_schwab_client()
    candidates = scan_for_candidates()

    if not candidates:
        print("No candidates found — run collector first during market hours.")
    else:
        scored      = score_candidates(candidates)
        all_results = run_gate(scored, client)

        for account_label, results in all_results.items():
            print(f"\n── Gate Results: Account {account_label} ──────────────")
            for r in results:
                status = "✓ APPROVED" if r.passed else "✗ BLOCKED "
                print(
                    f"  {status} | {r.scored.candidate.symbol} "
                    f"{r.scored.candidate.expiry} "
                    f"score={r.scored.total_score:.1f} | "
                    f"{r.blocking_reason or 'all rules passed'}"
                )

            approved = [r for r in results if r.passed]
            print(f"\n  {len(approved)}/{len(results)} candidates approved")
            if approved:
                best = approved[0]
                print(f"  Top: {best.scored.candidate.symbol} "
                      f"{best.scored.candidate.expiry} — "
                      f"${best.scored.candidate.net_credit:.2f} credit | "
                      f"score {best.scored.total_score:.1f}/100")