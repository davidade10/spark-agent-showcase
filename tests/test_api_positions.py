"""
tests/test_api_positions.py — Tests for the GET /positions endpoint.

Uses FastAPI's TestClient (via httpx) with a mocked SQLAlchemy engine so
no live database is required.  Verifies shape, data transformations, and
edge cases (null fill_credit, JSON legs parsing, datetime serialisation).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from approval_ui.api import app


# ── Helpers ───────────────────────────────────────────────────────────────────

_OPENED = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)


def _mock_row(data: dict):
    """Wrap a plain dict so it behaves like a SQLAlchemy Row (_mapping attr)."""
    row = MagicMock()
    row._mapping = data
    return row


def _make_engine(rows: list[dict]):
    """Return a mock engine whose connect() yields the given rows from fetchall()."""
    engine = MagicMock()

    result  = MagicMock()
    result.fetchall.return_value = [_mock_row(r) for r in rows]

    conn = MagicMock()
    conn.execute.return_value = result
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__  = MagicMock(return_value=False)

    engine.connect.return_value = conn
    # begin() used by startup migration — return a no-op context manager
    begin_conn = MagicMock()
    begin_conn.__enter__ = MagicMock(return_value=begin_conn)
    begin_conn.__exit__  = MagicMock(return_value=False)
    engine.begin.return_value = begin_conn

    return engine


def _base_row(**overrides) -> dict:
    base = {
        "id":           1,
        "account_id":   "5760",
        "symbol":       "SPY",
        "strategy":     "IRON_CONDOR",
        "expiry":       "2026-04-17",
        "dte":          33,
        "fill_credit":  0.75,
        "net_delta":    None,
        "unrealized_pnl": None,
        "opened_at":    _OPENED,
        "status":       "open",
        "legs":         None,
        "meta":         None,
        "max_risk":     None,
        "position_key": "SPY:2026-04-17:530-535:560-565:1",
        "qty":          1,
    }
    base.update(overrides)
    return base


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestGetPositions:
    def test_empty_positions_returns_empty_list(self):
        engine = _make_engine([])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/positions")

        assert resp.status_code == 200
        body = resp.json()
        assert "positions" in body
        assert body["positions"] == []

    def test_position_row_has_required_fields(self):
        engine = _make_engine([_base_row()])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/positions")

        assert resp.status_code == 200
        p = resp.json()["positions"][0]
        for field in ("id", "symbol", "strategy", "expiry", "fill_credit",
                      "entry_credit", "opened_at", "status", "qty"):
            assert field in p, f"Missing field: {field}"

    def test_fill_credit_aliased_as_entry_credit(self):
        engine = _make_engine([_base_row(fill_credit=1.20)])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/positions")

        p = resp.json()["positions"][0]
        assert p["fill_credit"]  == pytest.approx(1.20)
        assert p["entry_credit"] == pytest.approx(1.20)

    def test_profit_pct_computed_when_pnl_and_credit_present(self):
        # profit_pct = unrealized_pnl / fill_credit * 100
        engine = _make_engine([_base_row(fill_credit=0.80, unrealized_pnl=0.40)])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/positions")

        p = resp.json()["positions"][0]
        assert p["profit_pct"] == pytest.approx(50.0, abs=0.1)

    def test_profit_pct_none_when_fill_credit_null(self):
        engine = _make_engine([_base_row(fill_credit=None, unrealized_pnl=0.30)])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/positions")

        p = resp.json()["positions"][0]
        assert p["profit_pct"] is None

    def test_legs_json_string_parsed_to_list(self):
        legs_data = [{"option_type": "P", "strike": 530.0, "long_qty": 1}]
        engine = _make_engine([_base_row(legs=json.dumps(legs_data))])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/positions")

        p = resp.json()["positions"][0]
        assert isinstance(p["legs"], list)
        assert p["legs"][0]["strike"] == 530.0

    def test_opened_at_datetime_serialised_to_iso_string(self):
        engine = _make_engine([_base_row(opened_at=_OPENED)])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/positions")

        p = resp.json()["positions"][0]
        # Should be a string, not a datetime object
        assert isinstance(p["opened_at"], str)
        assert "2026-03-01" in p["opened_at"]
