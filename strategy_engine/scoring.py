"""
scoring.py — Strategy Engine
Scores and ranks IronCondorCandidate objects from candidates.py.

Four scoring dimensions (each 0–25 points, total 0–100):

  1. IV Rank score    (25 pts) — higher IV = more premium available
  2. Credit/Width     (25 pts) — premium collected vs risk taken
  3. Delta score      (25 pts) — closeness to target 0.16 delta
  4. DTE score        (25 pts) — closeness to 30-45 DTE sweet spot

Weights are equal at 25 points each. This is intentional — no single
dimension should dominate. If IV rank is very high but credit is tiny,
the trade shouldn't automatically win. All four factors matter equally.

Output: same list of IronCondorCandidate objects, sorted highest → lowest score.
Each candidate gets a ScoredCandidate wrapper with the score breakdown attached.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from strategy_engine.candidates import IronCondorCandidate

logger = logging.getLogger(__name__)

# ── Scoring constants ─────────────────────────────────────────────────────────

# Each dimension is worth this many points
POINTS_PER_DIMENSION = 25

# IV rank thresholds
IV_RANK_HIGH = 50.0   # above this → full IV rank score
IV_RANK_LOW  = 20.0   # below this → zero IV rank score

# Credit-to-width thresholds
# A $5-wide spread collecting $1.50 = 30% ratio (excellent)
# A $5-wide spread collecting $0.40 = 8%  ratio (minimum acceptable)
CREDIT_WIDTH_HIGH = 0.30   # 30% → full credit score
CREDIT_WIDTH_LOW  = 0.08   # 8%  → zero credit score

# Delta thresholds (absolute value)
TARGET_DELTA  = 0.16
DELTA_PERFECT = 0.01   # within this of target → full score
DELTA_MAX_DEV = 0.06   # this far from target → zero score

# DTE sweet spot
DTE_IDEAL_LOW  = 30    # bottom of sweet spot
DTE_IDEAL_HIGH = 45    # top of sweet spot
DTE_MAX_DEV    = 15    # this far outside sweet spot → zero score


# ── Scored wrapper ────────────────────────────────────────────────────────────
@dataclass
class ScoredCandidate:
    """
    Wraps an IronCondorCandidate with its score breakdown.
    This is what flows into rules_gate.py and the LLM layer.

    The score breakdown is stored separately so every downstream
    component can see exactly why a candidate ranked where it did.
    This is the auditable record — you can always trace a trade
    recommendation back to its specific score components.
    """
    candidate:          IronCondorCandidate

    # Total score (0–100)
    total_score:        float

    # Component scores (each 0–25)
    iv_rank_score:      float
    credit_width_score: float
    delta_score:        float
    dte_score:          float

    # Human-readable explanation of each component
    score_notes:        list[str]

    @property
    def symbol(self) -> str:
        return self.candidate.symbol

    @property
    def expiry(self) -> str:
        return self.candidate.expiry

    @property
    def dte(self) -> int:
        return self.candidate.dte

    @property
    def net_credit(self) -> float:
        return self.candidate.net_credit

    @property
    def max_loss(self) -> float:
        return self.candidate.max_loss

    def summary_line(self) -> str:
        """One-line summary for logging and display."""
        return (
            f"{self.candidate.symbol} {self.candidate.expiry} "
            f"DTE={self.candidate.dte} | "
            f"Score={self.total_score:.1f}/100 | "
            f"Credit=${self.candidate.net_credit:.2f} | "
            f"IV_rank={self.iv_rank_score:.1f} "
            f"C/W={self.credit_width_score:.1f} "
            f"Δ={self.delta_score:.1f} "
            f"DTE={self.dte_score:.1f}"
        )


# ── Scoring functions ─────────────────────────────────────────────────────────
def _score_iv_rank(iv_rank: Optional[float]) -> tuple[float, str]:
    """
    Scores the IV rank dimension (0–25 points).

    High IV rank means options are expensive relative to their recent history.
    When IV is elevated, premium sellers collect more credit for the same risk.
    This is the single most important edge in an iron condor strategy.

    IV rank None (insufficient history) → neutral score of 12.5 (half points).
    We don't penalize or reward — we just treat it as unknown.

    Scale:
      IV rank ≥ 50 → 25 points (full score)
      IV rank = 35 → 18.75 points (interpolated)
      IV rank ≤ 20 → 0 points
      IV rank None → 12.5 points (neutral)
    """
    if iv_rank is None:
        return 12.5, "IV rank N/A — neutral score (insufficient history)"

    if iv_rank >= IV_RANK_HIGH:
        score = POINTS_PER_DIMENSION
        note  = f"IV rank {iv_rank:.0f} ≥ {IV_RANK_HIGH:.0f} — full score"
    elif iv_rank <= IV_RANK_LOW:
        score = 0.0
        note  = f"IV rank {iv_rank:.0f} ≤ {IV_RANK_LOW:.0f} — zero score"
    else:
        # Linear interpolation between low and high thresholds
        score = (
            (iv_rank - IV_RANK_LOW) /
            (IV_RANK_HIGH - IV_RANK_LOW) *
            POINTS_PER_DIMENSION
        )
        note = f"IV rank {iv_rank:.0f} → {score:.1f}/{POINTS_PER_DIMENSION} pts"

    return round(score, 2), note


def _score_credit_width(net_credit: float, spread_width: float) -> tuple[float, str]:
    """
    Scores the credit-to-width ratio dimension (0–25 points).

    Credit/width = net credit collected ÷ spread width.
    Measures how efficiently you're being paid for the risk you're taking.

    A $5-wide spread should collect at least $0.40 (8% ratio) to be worth
    trading after commissions. The best setups collect 25-35%+.

    Scale:
      ratio ≥ 30% → 25 points
      ratio = 19% → ~14 points (interpolated)
      ratio ≤  8% → 0 points
    """
    if spread_width <= 0:
        return 0.0, "Invalid spread width — zero score"

    ratio = net_credit / spread_width

    if ratio >= CREDIT_WIDTH_HIGH:
        score = POINTS_PER_DIMENSION
        note  = (
            f"Credit/width {ratio:.1%} ≥ {CREDIT_WIDTH_HIGH:.0%} — full score"
        )
    elif ratio <= CREDIT_WIDTH_LOW:
        score = 0.0
        note  = (
            f"Credit/width {ratio:.1%} ≤ {CREDIT_WIDTH_LOW:.0%} — zero score"
        )
    else:
        score = (
            (ratio - CREDIT_WIDTH_LOW) /
            (CREDIT_WIDTH_HIGH - CREDIT_WIDTH_LOW) *
            POINTS_PER_DIMENSION
        )
        note = (
            f"Credit/width {ratio:.1%} → {score:.1f}/{POINTS_PER_DIMENSION} pts"
        )

    return round(score, 2), note


def _score_delta(
    short_put_delta: float,
    short_call_delta: float,
) -> tuple[float, str]:
    """
    Scores the delta dimension (0–25 points).

    Measures how close both short strikes are to the 0.16 target.
    We score the average absolute deviation of both short strikes.

    0.16 delta means approximately a 16% probability of the strike
    being touched at expiry — the right balance between premium
    collected and probability of staying OTM.

    Scale:
      avg deviation ≤ 0.01 → 25 points (perfectly on target)
      avg deviation = 0.035 → ~12.5 points (interpolated)
      avg deviation ≥ 0.06 → 0 points (too far from target)
    """
    put_dev  = abs(abs(short_put_delta)  - TARGET_DELTA)
    call_dev = abs(abs(short_call_delta) - TARGET_DELTA)
    avg_dev  = (put_dev + call_dev) / 2

    if avg_dev <= DELTA_PERFECT:
        score = POINTS_PER_DIMENSION
        note  = (
            f"Δput={short_put_delta:.3f} Δcall={short_call_delta:.3f} "
            f"— on target, full score"
        )
    elif avg_dev >= DELTA_MAX_DEV:
        score = 0.0
        note  = (
            f"Δput={short_put_delta:.3f} Δcall={short_call_delta:.3f} "
            f"— too far from target {TARGET_DELTA}, zero score"
        )
    else:
        score = (
            (1 - (avg_dev - DELTA_PERFECT) /
             (DELTA_MAX_DEV - DELTA_PERFECT)) *
            POINTS_PER_DIMENSION
        )
        note = (
            f"Δput={short_put_delta:.3f} Δcall={short_call_delta:.3f} "
            f"avg dev={avg_dev:.3f} → {score:.1f}/{POINTS_PER_DIMENSION} pts"
        )

    return round(score, 2), note


def _score_dte(dte: int) -> tuple[float, str]:
    """
    Scores the DTE dimension (0–25 points).

    The 30–45 DTE window is optimal for iron condors because:
      - Theta decay accelerates in the last 45 days (good for sellers)
      - Gamma risk is still manageable (bad gamma spikes inside 21 DTE)
      - Enough time for the trade to work without excessive exposure

    Inside the sweet spot → full score.
    Outside → score decays linearly to zero at 15 days beyond either edge.

    Scale:
      30 ≤ DTE ≤ 45 → 25 points (full score)
      DTE = 50      → ~16.7 points (5 days outside high end)
      DTE = 21      → ~10 points  (9 days outside low end)
      DTE ≤ 15 or ≥ 60 → 0 points
    """
    if DTE_IDEAL_LOW <= dte <= DTE_IDEAL_HIGH:
        score = POINTS_PER_DIMENSION
        note  = f"DTE={dte} in sweet spot {DTE_IDEAL_LOW}–{DTE_IDEAL_HIGH} — full score"
    else:
        if dte < DTE_IDEAL_LOW:
            deviation = DTE_IDEAL_LOW - dte
        else:
            deviation = dte - DTE_IDEAL_HIGH

        if deviation >= DTE_MAX_DEV:
            score = 0.0
            note  = (
                f"DTE={dte} — {deviation} days outside sweet spot, zero score"
            )
        else:
            score = (
                (1 - deviation / DTE_MAX_DEV) * POINTS_PER_DIMENSION
            )
            note = (
                f"DTE={dte} — {deviation} days outside sweet spot "
                f"→ {score:.1f}/{POINTS_PER_DIMENSION} pts"
            )

    return round(score, 2), note


# ── Main scorer ───────────────────────────────────────────────────────────────
def score_candidate(candidate: IronCondorCandidate) -> ScoredCandidate:
    """
    Scores a single IronCondorCandidate across all four dimensions.
    Returns a ScoredCandidate with the full breakdown attached.
    """
    iv_score,    iv_note    = _score_iv_rank(candidate.iv_rank)
    cw_score,    cw_note    = _score_credit_width(
                                  candidate.net_credit,
                                  candidate.spread_width,
                              )
    delta_score, delta_note = _score_delta(
                                  candidate.short_put_delta,
                                  candidate.short_call_delta,
                              )
    dte_score,   dte_note   = _score_dte(candidate.dte)

    total = round(iv_score + cw_score + delta_score + dte_score, 2)

    return ScoredCandidate(
        candidate          = candidate,
        total_score        = total,
        iv_rank_score      = iv_score,
        credit_width_score = cw_score,
        delta_score        = delta_score,
        dte_score          = dte_score,
        score_notes        = [iv_note, cw_note, delta_note, dte_note],
    )


def score_candidates(
    candidates: list[IronCondorCandidate],
) -> list[ScoredCandidate]:
    """
    Scores and ranks a list of candidates.
    Returns ScoredCandidate list sorted highest → lowest total score.

    This is the main entry point called by the orchestrator in main.py
    and by rules_gate.py after filtering.
    """
    if not candidates:
        logger.warning("score_candidates called with empty list")
        return []

    scored = [score_candidate(c) for c in candidates]
    scored.sort(key=lambda s: s.total_score, reverse=True)

    logger.info(
        f"Scored {len(scored)} candidates — "
        f"top: {scored[0].summary_line()}"
    )

    return scored


# ── Manual test run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    from strategy_engine.candidates import scan_for_candidates

    print("Scanning for candidates then scoring...\n")

    candidates = scan_for_candidates()

    if not candidates:
        print("No candidates found — run collector first during market hours.")
    else:
        scored = score_candidates(candidates)

        print(f"Scored {len(scored)} candidates:\n")
        for rank, s in enumerate(scored, 1):
            print(f"  #{rank} {s.summary_line()}")
            for note in s.score_notes:
                print(f"      {note}")
            print()

        print("─" * 60)
        print(f"Top candidate: {scored[0].candidate.symbol} "
              f"{scored[0].candidate.expiry} — "
              f"score {scored[0].total_score:.1f}/100")
        print(f"  P${scored[0].candidate.short_put_strike:.0f}/"
              f"{scored[0].candidate.long_put_strike:.0f} "
              f"C${scored[0].candidate.short_call_strike:.0f}/"
              f"{scored[0].candidate.long_call_strike:.0f}")
        print(f"  Credit: ${scored[0].candidate.net_credit:.2f} | "
              f"Max loss: ${scored[0].candidate.max_loss:.2f}")