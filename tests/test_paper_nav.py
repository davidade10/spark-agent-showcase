"""
tests/test_paper_nav.py

Tests for get_paper_nav() and the PAPER NAV field in GET /accounts.

Coverage:
  1. get_paper_nav returns PAPER_ACCOUNT_STARTING_BALANCE when trade_outcomes is empty
  2. get_paper_nav returns starting balance plus sum of closed P&L when PAPER rows exist
  3. get_paper_nav ignores non-PAPER rows (verified via SQL WHERE filter)
  4. GET /accounts returns dynamically computed PAPER NAV, not the static .env value
  5. GET /accounts response structure is fully unchanged after the fix

No live database required — all DB interactions are mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from approval_ui.api import app, get_paper_nav
from config import PAPER_ACCOUNT_STARTING_BALANCE


# ── Helpers ────────────────────────────────────────────────────────────────────

def _conn_with_pnl(total_pnl: float) -> MagicMock:
    """Mock conn whose execute().fetchone() returns a row with .total_pnl."""
    row = MagicMock()
    row.total_pnl = total_pnl
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = row
    return conn


def _make_accounts_engine(paper_pnl: float) -> MagicMock:
    """
    Mock engine for GET /accounts with two sequential execute() calls:
      1. SELECT account_id, COUNT(*) FROM positions  → fetchall() → []
      2. SELECT COALESCE(SUM(pnl), 0) FROM trade_outcomes → fetchone() → pnl row
    """
    pos_result = MagicMock()
    pos_result.fetchall.return_value = []

    pnl_row = MagicMock()
    pnl_row.total_pnl = paper_pnl
    pnl_result = MagicMock()
    pnl_result.fetchone.return_value = pnl_row

    conn = MagicMock()
    conn.execute.side_effect = [pos_result, pnl_result]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__  = MagicMock(return_value=False)

    begin_conn = MagicMock()
    begin_conn.__enter__ = MagicMock(return_value=begin_conn)
    begin_conn.__exit__  = MagicMock(return_value=False)

    engine = MagicMock()
    engine.connect.return_value = conn
    engine.begin.return_value   = begin_conn
    return engine


def _paper_entry(resp_body: dict) -> dict:
    return next(a for a in resp_body["accounts"] if a["account_id"] == "PAPER")


# ── Test 1-3: get_paper_nav unit tests ────────────────────────────────────────

class TestGetPaperNav:

    def test_returns_starting_balance_when_no_outcomes(self):
        """COALESCE(SUM, 0) = 0 → returns PAPER_ACCOUNT_STARTING_BALANCE exactly."""
        conn = _conn_with_pnl(0.0)
        result = get_paper_nav(conn)
        assert result == PAPER_ACCOUNT_STARTING_BALANCE

    def test_returns_starting_balance_plus_realized_pnl(self):
        """Positive cumulative P&L is added to the starting balance."""
        conn = _conn_with_pnl(350.0)
        result = get_paper_nav(conn)
        assert result == round(PAPER_ACCOUNT_STARTING_BALANCE + 350.0, 2)

    def test_handles_negative_pnl_correctly(self):
        """Net losses reduce NAV below the starting balance."""
        conn = _conn_with_pnl(-200.0)
        result = get_paper_nav(conn)
        assert result == round(PAPER_ACCOUNT_STARTING_BALANCE - 200.0, 2)

    def test_query_filters_by_paper_account_id(self):
        """
        SQL must include account_id = 'PAPER' in the WHERE clause so that
        outcomes from live accounts are never counted toward paper NAV.
        """
        conn = _conn_with_pnl(0.0)
        get_paper_nav(conn)
        sql_text = str(conn.execute.call_args[0][0])
        assert "account_id" in sql_text
        assert "PAPER"      in sql_text


# ── Test 4-5: GET /accounts endpoint tests ────────────────────────────────────

class TestGetAccountsPaperNav:

    def test_paper_nav_uses_db_not_static_env(self):
        """
        PAPER NAV must equal PAPER_ACCOUNT_STARTING_BALANCE + realized P&L
        from the DB, not the static PAPER_ACCOUNT_NAV env value.

        With paper_pnl=500.0 the expected nav is starting_balance + 500,
        which differs from the static env default of 20000.
        """
        engine = _make_accounts_engine(paper_pnl=500.0)
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/accounts")

        assert resp.status_code == 200
        paper = _paper_entry(resp.json())
        expected = round(PAPER_ACCOUNT_STARTING_BALANCE + 500.0, 2)
        assert paper["nav"] == expected

    def test_paper_account_response_structure_unchanged(self):
        """
        All expected keys are present in the PAPER account dict and their
        types match — only the nav value changes, not the schema.
        """
        engine = _make_accounts_engine(paper_pnl=0.0)
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/accounts")

        assert resp.status_code == 200
        paper = _paper_entry(resp.json())

        expected_keys = {
            "account_id", "type", "nav", "open_positions",
            "buying_power", "daily_pnl", "total_credit",
            "total_margin", "total_pnl", "last_synced",
        }
        assert set(paper.keys()) == expected_keys
        assert paper["account_id"]     == "PAPER"
        assert paper["type"]           == "PAPER"
        assert paper["open_positions"] == 0
        assert paper["buying_power"]   is None
        assert paper["daily_pnl"]      is None
        assert paper["total_credit"]   is None
        assert paper["total_margin"]   is None
        assert paper["total_pnl"]      is None
