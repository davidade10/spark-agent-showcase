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
  7. FOMC proximity            — no new positions within 2 days of FOMC
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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, HARD_RULES
from data_layer.events_calendar import is_earnings_within_days, is_fomc_within_days
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
            open_condors    = HARD_RULES["max_open_condors"],
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
def _check_max_open_condors(ctx: AccountContext) -> Optional[str]:
    """Returns failure reason string if rule fails, None if passes."""
    if ctx.open_condors >= HARD_RULES["max_open_condors"]:
        return (
            f"Already have {ctx.open_condors} open condors "
            f"(max {HARD_RULES['max_open_condors']})"
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
    Checks that max loss on this trade doesn't exceed 1% of NAV.
    Max loss per contract = spread_width - net_credit (per share × 100).
    We assume 1 contract (100 shares) as the default position size.
    """
    if ctx.nav <= 0:
        return "Cannot verify position risk — NAV unavailable"

    max_loss_dollars = scored.candidate.max_loss * 100  # per contract
    risk_pct         = max_loss_dollars / ctx.nav

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

    existing_risk    = ctx.correlated_risk.get(group, 0.0)
    new_risk_dollars = scored.candidate.max_loss * 100
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


# ── Database writer ───────────────────────────────────────────────────────────
def _write_to_trade_candidates(
    conn,
    scored:          ScoredCandidate,
    gate_result:     str,
    blocking_rule:   Optional[str],
    blocking_reason: Optional[str],
) -> int:
    """
    Writes the candidate and gate result to trade_candidates table.
    Returns the database ID of the inserted row.

    Written regardless of pass/fail — the blocked candidates are just as
    important for the audit trail as the approved ones.
    """
    c = scored.candidate

    candidate_json = {
        "symbol":             c.symbol,
        "expiry":             c.expiry,
        "dte":                c.dte,
        "long_put_strike":    c.long_put_strike,
        "short_put_strike":   c.short_put_strike,
        "short_call_strike":  c.short_call_strike,
        "long_call_strike":   c.long_call_strike,
        "net_credit":         c.net_credit,
        "spread_width":       c.spread_width,
        "max_loss":           c.max_loss,
        "underlying_price":   c.underlying_price,
        "iv_rank":            c.iv_rank,
        "short_put_delta":    c.short_put_delta,
        "short_call_delta":   c.short_call_delta,
    }

    score_json = {
        "total_score":        scored.total_score,
        "iv_rank_score":      scored.iv_rank_score,
        "credit_width_score": scored.credit_width_score,
        "delta_score":        scored.delta_score,
        "dte_score":          scored.dte_score,
        "score_notes":        scored.score_notes,
        "blocking_rule":      blocking_rule,
        "blocking_reason":    blocking_reason,
    }

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
            account_id
        ) VALUES (
            :created_at,
            :snapshot_id,
            :symbol,
            'iron_condor',
            :score,
            :candidate_json,
            :score_json,
            :gate_result,
            'primary'
        )
        RETURNING id
    """), {
        "created_at":      datetime.now(timezone.utc),
        "snapshot_id":     c.snapshot_id,
        "symbol":          c.symbol,
        "score":           scored.total_score,
        "candidate_json":  json.dumps(candidate_json),
        "score_json":      json.dumps(score_json),
        "gate_result":     gate_result,
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

    logger.info("Fetching account contexts from Schwab...")
    account_contexts = get_account_contexts(client)

    for account_label, ctx in account_contexts:
        logger.info(f"\n── Running gate for account {account_label} ──────────")
        results = []

        with engine.connect() as conn:
            for scored in scored_candidates:
                symbol          = scored.candidate.symbol
                blocking_rule   = None
                blocking_reason = None

                checks = [
                    ("max_open_condors",   lambda: _check_max_open_condors(ctx)),
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

                # Dedup: skip if this symbol/expiry already has a row
                try:
                    existing = conn.execute(text("""
                        SELECT id FROM trade_candidates
                        WHERE symbol = :symbol
                          AND candidate_json->>'expiry' = :expiry
                        LIMIT 1
                    """), {
                        "symbol": symbol,
                        "expiry": scored.candidate.expiry,
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
                        candidate_id = _write_to_trade_candidates(
                            conn, scored, gate_result, blocking_rule, blocking_reason
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