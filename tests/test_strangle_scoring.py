"""
tests/test_strangle_scoring.py

Tests for the STRANGLE scoring dimensions in strategy_engine/scoring.py.

Verifies:
  - High IV rank + balanced deltas + good DTE + good credit → score ≥ 75
  - Low IV rank + poor conditions → score < 40
  - Each dimension scores within its declared max
  - Event proximity: earnings → 0 pts; no earnings → 10 pts
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from strategy_engine.candidates import StrangleCandidate
from strategy_engine.scoring import (
    score_strangle,
    _score_strangle_iv_rank,
    _score_strangle_credit_pct,
    _score_strangle_dte,
    _score_strangle_delta_symmetry,
    _score_strangle_events,
)


# ── Fixture helpers ───────────────────────────────────────────────────────────

def _make_candidate(
    iv_rank=75.0,
    dte=35,
    net_credit=3.50,
    underlying_price=175.0,
    short_put_delta=-0.22,
    short_call_delta=0.22,
    symbol="NVDA",
) -> StrangleCandidate:
    return StrangleCandidate(
        strategy          = "STRANGLE",
        symbol            = symbol,
        snapshot_id       = 1,
        expiry            = "2026-04-17",
        dte               = dte,
        short_put_strike  = 140.0,
        short_call_strike = 210.0,
        short_put_delta   = short_put_delta,
        short_call_delta  = short_call_delta,
        short_put_credit  = 1.75,
        short_call_credit = 1.75,
        net_credit        = net_credit,
        iv_rank           = iv_rank,
        underlying_price  = underlying_price,
    )


# ── High-quality candidate: expect ≥ 75 ──────────────────────────────────────

class TestHighQualityStrangle:

    def test_high_quality_score_gte_75(self):
        """
        IV rank 75, balanced deltas (0.22/0.22), 35 DTE, net_credit 3.50 on
        underlying price 175 → credit_pct = 2.0% → expect score ≥ 75.
        """
        cand = _make_candidate(
            iv_rank=75.0,
            dte=35,
            net_credit=3.50,
            underlying_price=175.0,
            short_put_delta=-0.22,
            short_call_delta=0.22,
        )
        with patch("strategy_engine.scoring._is_earnings_within_days", return_value=False):
            scored = score_strangle(cand)

        assert scored.total_score >= 75, (
            f"Expected ≥ 75 but got {scored.total_score:.1f}. "
            f"Notes: {scored.score_notes}"
        )

    def test_high_quality_dimensions_nonzero(self):
        """All five dimensions should contribute positively for a good candidate."""
        cand = _make_candidate(iv_rank=75.0, dte=35, net_credit=3.50,
                               underlying_price=175.0)
        with patch("strategy_engine.scoring._is_earnings_within_days", return_value=False):
            scored = score_strangle(cand)

        assert scored.iv_rank_score > 0
        assert scored.credit_width_score > 0
        assert scored.dte_score > 0
        assert scored.delta_score > 0        # symmetry
        assert scored.call_delta_score > 0   # event proximity


# ── Low IV rank: expect < 40 ──────────────────────────────────────────────────

class TestLowIvRankStrangle:

    def test_low_iv_rank_score_lt_40(self):
        """
        IV rank 35, DTE 55 (outside 21–50 window → 0), low credit, poor symmetry.
        All dimensions deliberately weak → expect score < 40.
        """
        cand = _make_candidate(
            iv_rank=35.0,
            dte=55,
            net_credit=0.90,
            underlying_price=175.0,
            short_put_delta=-0.22,
            short_call_delta=0.15,  # asymmetry = |0.22 − 0.15| = 0.07 → low
        )
        with patch("strategy_engine.scoring._is_earnings_within_days", return_value=False):
            scored = score_strangle(cand)

        assert scored.total_score < 40, (
            f"Expected < 40 but got {scored.total_score:.1f}. "
            f"Notes: {scored.score_notes}"
        )

    def test_low_iv_rank_iv_score_is_zero(self):
        """IV rank 35 must produce 0 IV rank score."""
        score, _ = _score_strangle_iv_rank(35.0)
        assert score == 0.0

    def test_none_iv_rank_neutral(self):
        """None IV rank must produce neutral 15 pts (don't penalize on missing data)."""
        score, _ = _score_strangle_iv_rank(None)
        assert score == 15.0


# ── IV rank dimension ─────────────────────────────────────────────────────────

class TestStrangleIvRankScoring:

    def test_iv_rank_80_full(self):
        score, _ = _score_strangle_iv_rank(80.0)
        assert score == 30.0

    def test_iv_rank_90_full(self):
        score, _ = _score_strangle_iv_rank(90.0)
        assert score == 30.0

    def test_iv_rank_70_between_20_and_30(self):
        score, _ = _score_strangle_iv_rank(70.0)
        assert 20.0 < score < 30.0

    def test_iv_rank_50_between_0_and_20(self):
        score, _ = _score_strangle_iv_rank(50.0)
        assert 0.0 < score <= 20.0

    def test_iv_rank_39_zero(self):
        score, _ = _score_strangle_iv_rank(39.0)
        assert score == 0.0


# ── Credit pct dimension ──────────────────────────────────────────────────────

class TestStrangleCreditPctScoring:

    def test_credit_pct_above_2_full(self):
        score, _ = _score_strangle_credit_pct(4.0, 175.0)   # 2.28% ≥ 2.0
        assert score == 25.0

    def test_credit_pct_below_0_8_zero(self):
        score, _ = _score_strangle_credit_pct(1.0, 175.0)   # 0.57% < 0.8
        assert score == 0.0

    def test_credit_pct_midrange_interpolated(self):
        score, _ = _score_strangle_credit_pct(2.10, 175.0)  # 1.2% in 0.8–2.0
        assert 0.0 < score < 25.0

    def test_zero_underlying_price_zero(self):
        score, _ = _score_strangle_credit_pct(2.0, 0.0)
        assert score == 0.0


# ── DTE dimension ─────────────────────────────────────────────────────────────

class TestStrangleDteScoring:

    def test_dte_35_full(self):
        score, _ = _score_strangle_dte(35)
        assert score == 20.0

    def test_dte_30_full(self):
        score, _ = _score_strangle_dte(30)
        assert score == 20.0

    def test_dte_40_full(self):
        score, _ = _score_strangle_dte(40)
        assert score == 20.0

    def test_dte_20_zero(self):
        score, _ = _score_strangle_dte(20)
        assert score == 0.0

    def test_dte_51_zero(self):
        score, _ = _score_strangle_dte(51)
        assert score == 0.0

    def test_dte_25_partial(self):
        score, _ = _score_strangle_dte(25)
        assert 0.0 < score < 20.0

    def test_dte_45_partial(self):
        score, _ = _score_strangle_dte(45)
        assert 0.0 < score < 20.0


# ── Delta symmetry dimension ──────────────────────────────────────────────────

class TestStrangleDeltaSymmetry:

    def test_perfectly_balanced_full(self):
        score, _ = _score_strangle_delta_symmetry(0.22, -0.22)
        assert score == 15.0

    def test_small_asymmetry_full(self):
        score, _ = _score_strangle_delta_symmetry(0.22, -0.20)  # asymmetry = 0.02
        assert score == 15.0

    def test_large_asymmetry_zero(self):
        score, _ = _score_strangle_delta_symmetry(0.22, -0.10)  # asymmetry = 0.12
        assert score == 0.0

    def test_mid_asymmetry_partial(self):
        score, _ = _score_strangle_delta_symmetry(0.22, -0.17)  # asymmetry = 0.05
        assert 0.0 < score < 15.0


# ── Event proximity dimension ─────────────────────────────────────────────────

class TestStrangleEventProximity:

    def test_earnings_within_30d_zero(self):
        with patch("strategy_engine.scoring._is_earnings_within_days", return_value=True):
            score, _ = _score_strangle_events("NVDA")
        assert score == 0.0

    def test_no_earnings_full(self):
        with patch("strategy_engine.scoring._is_earnings_within_days", return_value=False):
            score, _ = _score_strangle_events("NVDA")
        assert score == 10.0

    def test_check_exception_full_score(self):
        """Event check failure must not penalize the candidate."""
        with patch("strategy_engine.scoring._is_earnings_within_days",
                   side_effect=Exception("DB unavailable")):
            score, _ = _score_strangle_events("NVDA")
        assert score == 10.0

    def test_none_checker_full_score(self):
        """If events module unavailable at import, must return full score."""
        import strategy_engine.scoring as scoring_mod
        original = scoring_mod._is_earnings_within_days
        try:
            scoring_mod._is_earnings_within_days = None
            score, _ = _score_strangle_events("NVDA")
            assert score == 10.0
        finally:
            scoring_mod._is_earnings_within_days = original
