"""
tests/test_rules_gate.py

Tests for strategy_engine/rules_gate.py — max_open_condors rule.

Verifies that:
  - PAPER and live condor counts are looked up independently in the DB
  - PAPER limit is max_open_condors_paper (8)
  - Live limit is max_open_condors_live (5)
  - Block threshold is >= limit (not strictly >)
  - blocked_reason detail contains the correct account identifier and limit

No live database or Schwab client required — DB calls are mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from strategy_engine.candidates import IronCondorCandidate
from strategy_engine.rules_gate import _check_max_open_condors, _resolve_qty_for_candidate_json
from config import HARD_RULES


# ── Helpers ───────────────────────────────────────────────────────────────────

def _conn_returning_count(count: int) -> MagicMock:
    """
    Return a mock SQLAlchemy connection whose execute(...).fetchone() gives
    a row with .cnt == count.  Used to simulate the open-condor DB query.
    """
    row  = MagicMock()
    row.cnt = count
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = row
    return conn


# ── Paper account tests ───────────────────────────────────────────────────────

class TestPaperCondorLimit:

    PAPER_LIMIT = HARD_RULES["max_open_condors_paper"]   # 8

    def test_paper_7_condors_passes(self):
        """7 open PAPER condors is below the limit of 8 — gate should pass."""
        conn = _conn_returning_count(7)
        result = _check_max_open_condors(conn, "PAPER")
        assert result is None, f"Expected None (pass) but got: {result!r}"

    def test_paper_8_condors_blocked(self):
        """8 open PAPER condors equals the limit — gate must block (>= not >)."""
        conn = _conn_returning_count(8)
        result = _check_max_open_condors(conn, "PAPER")
        assert result is not None, "Expected a block reason but got None"

    def test_paper_0_condors_passes(self):
        """0 open condors — trivially passes."""
        conn = _conn_returning_count(0)
        assert _check_max_open_condors(conn, "PAPER") is None

    def test_paper_block_message_contains_paper_account_label(self):
        """blocked_reason detail must identify the account as 'PAPER account'."""
        conn   = _conn_returning_count(self.PAPER_LIMIT)
        result = _check_max_open_condors(conn, "PAPER")
        assert "PAPER account" in result

    def test_paper_block_message_contains_correct_count_and_limit(self):
        """Detail string must show the actual count and the paper-specific limit."""
        conn   = _conn_returning_count(self.PAPER_LIMIT)
        result = _check_max_open_condors(conn, "PAPER")
        assert str(self.PAPER_LIMIT) in result          # count value
        assert f"limit {self.PAPER_LIMIT}" in result    # limit value

    def test_paper_block_uses_paper_limit_not_live_limit(self):
        """
        A count that would block under the live limit (5) must NOT block under
        the paper limit (8). Confirms the correct limit is selected for PAPER.
        """
        live_limit  = HARD_RULES["max_open_condors_live"]   # 5
        paper_limit = HARD_RULES["max_open_condors_paper"]  # 8
        assert live_limit < paper_limit, "test precondition: live < paper"

        # Count = live_limit: would block for live, must pass for PAPER
        conn = _conn_returning_count(live_limit)
        assert _check_max_open_condors(conn, "PAPER") is None


# ── Live account tests ────────────────────────────────────────────────────────

class TestLiveCondorLimit:

    LIVE_LIMIT = HARD_RULES["max_open_condors_live"]   # 5

    def test_live_4_condors_passes(self):
        """4 open live condors is below the limit of 5 — gate should pass."""
        conn = _conn_returning_count(4)
        result = _check_max_open_condors(conn, "8096")
        assert result is None, f"Expected None (pass) but got: {result!r}"

    def test_live_5_condors_blocked(self):
        """5 open live condors equals the limit — gate must block (>= not >)."""
        conn = _conn_returning_count(5)
        result = _check_max_open_condors(conn, "8096")
        assert result is not None, "Expected a block reason but got None"

    def test_live_0_condors_passes(self):
        """0 open condors — trivially passes."""
        conn = _conn_returning_count(0)
        assert _check_max_open_condors(conn, "8096") is None

    def test_live_block_message_contains_account_id(self):
        """blocked_reason detail must identify the account by its ID."""
        conn   = _conn_returning_count(self.LIVE_LIMIT)
        result = _check_max_open_condors(conn, "8096")
        assert "8096" in result

    def test_live_block_message_contains_correct_count_and_limit(self):
        """Detail string must show the actual count and the live-specific limit."""
        conn   = _conn_returning_count(self.LIVE_LIMIT)
        result = _check_max_open_condors(conn, "8096")
        assert str(self.LIVE_LIMIT) in result
        assert f"limit {self.LIVE_LIMIT}" in result

    def test_live_block_uses_live_limit_not_paper_limit(self):
        """
        A count between live_limit and paper_limit must block for live
        but would not block for paper — confirms independent limits.
        """
        live_limit  = HARD_RULES["max_open_condors_live"]   # 5
        paper_limit = HARD_RULES["max_open_condors_paper"]  # 8
        # Count = live_limit: blocks live
        conn = _conn_returning_count(live_limit)
        assert _check_max_open_condors(conn, "8096") is not None

    def test_second_live_account_5760_also_uses_live_limit(self):
        """All live accounts (both 8096 and 5760) use the live limit."""
        conn = _conn_returning_count(self.LIVE_LIMIT)
        result = _check_max_open_condors(conn, "5760")
        assert result is not None  # 5 >= 5 → blocked

        conn_under = _conn_returning_count(self.LIVE_LIMIT - 1)
        assert _check_max_open_condors(conn_under, "5760") is None  # 4 < 5 → passes


# ── Independence test ─────────────────────────────────────────────────────────

class TestPaperLiveIndependence:

    def test_live_condors_do_not_affect_paper_gate(self):
        """
        5 condors on a live account (at the live limit) must NOT block a PAPER
        candidate. Each account's condor count is queried independently.

        This test calls _check_max_open_condors twice with different account_ids
        but the same count, confirming the limit selected depends on the account.
        """
        count_at_live_limit = HARD_RULES["max_open_condors_live"]  # 5

        # Live account with 5 condors → blocked
        conn_live = _conn_returning_count(count_at_live_limit)
        live_result = _check_max_open_condors(conn_live, "8096")
        assert live_result is not None, "Live account with 5 condors should be blocked"

        # PAPER account with same count (5) → should pass (paper limit is 8)
        conn_paper = _conn_returning_count(count_at_live_limit)
        paper_result = _check_max_open_condors(conn_paper, "PAPER")
        assert paper_result is None, (
            f"PAPER account with {count_at_live_limit} condors should pass "
            f"(paper limit is {HARD_RULES['max_open_condors_paper']})"
        )

    def test_db_query_uses_account_id_parameter(self):
        """
        Confirm the DB query is parameterized with the correct account_id.
        This verifies that counts are per-account, not global.
        """
        conn = _conn_returning_count(0)

        _check_max_open_condors(conn, "PAPER")
        call_params = conn.execute.call_args[0][1]
        assert call_params["account_id"] == "PAPER"

        conn2 = _conn_returning_count(0)
        _check_max_open_condors(conn2, "8096")
        call_params2 = conn2.execute.call_args[0][1]
        assert call_params2["account_id"] == "8096"


# ── candidate_json qty resolution ─────────────────────────────────────────────

class TestResolveQtyForCandidateJson:

    def test_iron_condor_default_qty(self):
        c = IronCondorCandidate(
            symbol="X",
            snapshot_id=1,
            expiry="2026-06-01",
            dte=30,
            long_put_strike=90.0,
            short_put_strike=95.0,
            short_call_strike=105.0,
            long_call_strike=110.0,
            long_put_mid=0.1,
            short_put_mid=0.4,
            short_call_mid=0.4,
            long_call_mid=0.1,
            short_put_delta=-0.2,
            short_call_delta=0.2,
            net_credit=0.5,
            spread_width=5.0,
            max_loss=4.5,
            underlying_price=100.0,
            iv_rank=50.0,
        )
        assert _resolve_qty_for_candidate_json(c) == 1

    def test_iron_condor_custom_qty(self):
        c = IronCondorCandidate(
            symbol="X",
            snapshot_id=1,
            expiry="2026-06-01",
            dte=30,
            long_put_strike=90.0,
            short_put_strike=95.0,
            short_call_strike=105.0,
            long_call_strike=110.0,
            long_put_mid=0.1,
            short_put_mid=0.4,
            short_call_mid=0.4,
            long_call_mid=0.1,
            short_put_delta=-0.2,
            short_call_delta=0.2,
            net_credit=0.5,
            spread_width=5.0,
            max_loss=4.5,
            underlying_price=100.0,
            iv_rank=50.0,
            qty=4,
        )
        assert _resolve_qty_for_candidate_json(c) == 4

    def test_non_ic_uses_position_size(self):

        class O:
            position_size = 3

        assert _resolve_qty_for_candidate_json(O()) == 3
