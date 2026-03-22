"""
tests/test_history.py

Tests for GET /history, mark_expired_candidates(), and related staleness behaviour.

Coverage:
  1. GET /history returns rows where trade_decisions exists
  2. GET /history does not return candidates with no decision row (empty DB result)
  3. GET /history?days=7 passes correct days param to SQL
  4. GET /history returns correct pnl for closed trades, null for rejected/open trades
  5. mark_expired_candidates() marks old approved candidates as expired via jsonb_set
  6. mark_expired_candidates() does not mark recently approved candidates (rowcount=0)
  7. mark_expired_candidates() threshold comes from APPROVAL_STALENESS_LIMIT_SECONDS
  8. mark_expired_candidates() returns 0 on DB error (never raises)
  9. GET /candidates does not return expired candidates (SQL WHERE clause check)
 10. is_stale uses APPROVAL_STALENESS_LIMIT_SECONDS/60 threshold, not hardcoded 15

No live database required — all DB interactions are mocked.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from approval_ui.api import app
from config import APPROVAL_STALENESS_LIMIT_SECONDS
from strategy_engine.candidate_lifecycle import mark_expired_candidates


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _mock_row(data: dict):
    """Wrap a dict so it behaves like a SQLAlchemy Row (_mapping attr)."""
    row = MagicMock()
    row._mapping = data
    return row


def _make_api_engine(rows: list[dict]):
    """
    Build a mock engine for API endpoint tests.
    connect() yields the given rows from fetchall() and fetchone().
    begin() returns a no-op context manager (used by startup migration).
    """
    engine    = MagicMock()
    result    = MagicMock()
    result.fetchall.return_value  = [_mock_row(r) for r in rows]
    result.fetchone.return_value  = _mock_row(rows[0]) if rows else None

    conn = MagicMock()
    conn.execute.return_value = result
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__  = MagicMock(return_value=False)
    engine.connect.return_value = conn

    begin_conn = MagicMock()
    begin_conn.__enter__ = MagicMock(return_value=begin_conn)
    begin_conn.__exit__  = MagicMock(return_value=False)
    engine.begin.return_value = begin_conn

    return engine


# ── Sample row fixtures ────────────────────────────────────────────────────────

_CLOSED_TRADE_ROW = {
    "id":           1,
    "symbol":       "IWM",
    "score":        72.0,
    "strategy":     "iron_condor",
    "account_id":   "PAPER",
    "created_at":   datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc),
    "candidate_json": json.dumps({
        "net_credit": 0.65,
        "expiry": "2026-04-17",
        "long_put_strike": 220,
        "short_put_strike": 225,
        "short_call_strike": 270,
        "long_call_strike": 275,
        "qty": 2,
    }),
    "llm_card":       None,
    "blocked_reason": None,
    "decision":     "approved",
    "decided_at":   datetime(2026, 3, 10, 10, 5, 0, tzinfo=timezone.utc),
    "reason":       None,
    "pnl":          45.0,
    "exit_reason":  "PROFIT_TARGET",
    "closed_at":    datetime(2026, 3, 15, 15, 0, 0, tzinfo=timezone.utc),
}

_REJECTED_TRADE_ROW = {
    **_CLOSED_TRADE_ROW,
    "id":           2,
    "decision":     "rejected",
    "reason":       "too wide",
    "pnl":          None,
    "exit_reason":  None,
    "closed_at":    None,
}


# ── Test 1: GET /history returns rows with decisions ──────────────────────────

class TestHistoryEndpoint:

    def test_returns_rows_with_decision(self):
        """GET /history returns candidates that have a trade_decisions row."""
        engine = _make_api_engine([_CLOSED_TRADE_ROW])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/history")

        assert resp.status_code == 200
        body = resp.json()
        assert "history" in body
        assert body["count"] == 1
        assert body["history"][0]["symbol"] == "IWM"
        assert body["history"][0]["qty"] == 2
        assert body["history"][0]["long_put_strike"] in (220, 220.0)

    def test_history_qty_null_when_absent_from_candidate_json(self):
        """Legacy rows without qty in candidate_json still return null qty cleanly."""
        row = {
            **_CLOSED_TRADE_ROW,
            "id": 901,
            "candidate_json": json.dumps({
                "net_credit": 0.65,
                "expiry": "2026-04-17",
                "long_put_strike": 220,
                "short_put_strike": 225,
                "short_call_strike": 270,
                "long_call_strike": 275,
            }),
        }
        engine = _make_api_engine([row])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/history")
        assert resp.status_code == 200
        assert resp.json()["history"][0]["qty"] is None

    def test_no_decision_rows_returns_empty(self):
        """GET /history returns count=0 when DB JOIN produces no rows."""
        engine = _make_api_engine([])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/history")

        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 0
        assert body["history"] == []

    def test_days_filter_passes_param_to_sql(self):
        """GET /history?days=7 must pass days=7 as a SQL bind parameter."""
        engine = _make_api_engine([])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/history?days=7")

        assert resp.status_code == 200
        assert resp.json()["days"] == 7
        conn = engine.connect.return_value
        params = conn.execute.call_args[0][1]
        assert params["days"] == 7

    def test_days_clamped_to_90(self):
        """days > 90 is silently clamped to 90."""
        engine = _make_api_engine([])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/history?days=999")

        assert resp.status_code == 200
        assert resp.json()["days"] == 90
        conn = engine.connect.return_value
        params = conn.execute.call_args[0][1]
        assert params["days"] == 90

    def test_pnl_present_for_closed_trade(self):
        """Closed trades return pnl and closed_at in the response."""
        engine = _make_api_engine([_CLOSED_TRADE_ROW])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/history")

        row = resp.json()["history"][0]
        assert row["pnl"] == 45.0
        assert row["closed_at"] is not None
        assert row["exit_reason"] == "PROFIT_TARGET"

    def test_pnl_null_for_rejected_trade(self):
        """Rejected trades return null pnl and null closed_at — correct and expected."""
        engine = _make_api_engine([_REJECTED_TRADE_ROW])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/history")

        row = resp.json()["history"][0]
        assert row["pnl"] is None
        assert row["closed_at"] is None

    def test_net_credit_and_expiry_extracted_from_candidate_json(self):
        """net_credit and expiry are pulled from candidate_json, not top-level columns."""
        engine = _make_api_engine([_CLOSED_TRADE_ROW])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/history")

        row = resp.json()["history"][0]
        assert row["net_credit"] == 0.65
        assert row["expiry"] == "2026-04-17"
        assert "candidate_json" not in row

    def test_default_days_is_30(self):
        """Default look-back is 30 days."""
        engine = _make_api_engine([])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/history")

        assert resp.json()["days"] == 30


# ── Test 5-8: mark_expired_candidates() unit tests ────────────────────────────

class TestMarkExpiredCandidates:

    def _conn(self, rowcount: int = 0) -> MagicMock:
        conn = MagicMock()
        conn.execute.return_value.rowcount = rowcount
        return conn

    def test_marks_old_candidates_as_expired(self):
        """When rowcount > 0, function returns count and calls execute once."""
        conn = self._conn(rowcount=3)
        result = mark_expired_candidates(conn)
        assert result == 3
        conn.execute.assert_called_once()

    def test_does_not_mark_recent_candidates(self):
        """When rowcount == 0, function returns 0."""
        conn = self._conn(rowcount=0)
        result = mark_expired_candidates(conn)
        assert result == 0

    def test_threshold_from_approval_staleness_limit(self):
        """
        The SQL bind parameter 'threshold' must equal
        timedelta(seconds=APPROVAL_STALENESS_LIMIT_SECONDS).
        This confirms no hardcoded 45, 2700, or minute value is used.
        """
        conn = self._conn(rowcount=0)
        mark_expired_candidates(conn)
        params = conn.execute.call_args[0][1]
        assert "threshold" in params
        assert params["threshold"] == timedelta(seconds=APPROVAL_STALENESS_LIMIT_SECONDS)

    def test_returns_zero_on_db_error(self):
        """DB errors are caught — function never raises, returns 0."""
        conn = MagicMock()
        conn.execute.side_effect = Exception("connection refused")
        result = mark_expired_candidates(conn)
        assert result == 0

    def test_sql_uses_jsonb_set(self):
        """UPDATE SQL must use jsonb_set to preserve existing card fields."""
        conn = self._conn(rowcount=0)
        mark_expired_candidates(conn)
        sql = str(conn.execute.call_args[0][0])
        assert "jsonb_set" in sql
        assert "expired" in sql


# ── Test 9: GET /candidates SQL excludes expired rows ─────────────────────────

class TestCandidatesExcludesExpired:

    def test_where_clause_contains_expired(self):
        """
        The WHERE clause in GET /candidates must include 'expired' in the
        NOT IN list so expired candidates never reach the approval queue.
        """
        engine = _make_api_engine([])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                client.get("/candidates")

        conn = engine.connect.return_value
        sql = str(conn.execute.call_args[0][0])
        assert "expired" in sql


# ── Test 10: is_stale uses APPROVAL_STALENESS_LIMIT_SECONDS ──────────────────

class TestCandidateStalenessThreshold:

    def test_is_stale_uses_config_threshold_not_hardcoded_15(self):
        """
        is_stale must be False for a candidate whose age is between 15 and
        APPROVAL_STALENESS_LIMIT_SECONDS/60 minutes (currently 20 min).

        With APPROVAL_STALENESS_LIMIT_SECONDS=1200 (20 min):
          - A 16-minute-old candidate is NOT stale (16 < 20).
          - With the old hardcoded > 15 it would have been stale (16 > 15).

        This test breaks if the threshold reverts to 15.
        """
        stale_limit_min = APPROVAL_STALENESS_LIMIT_SECONDS / 60
        # Pick an age that is above 15 but below the correct limit.
        # If the limit is exactly 20 min (1200 s), 16 min is in the gap.
        gap_age_min = 16  # > 15 (old threshold), < 20 (correct threshold)

        # Only run the meaningful assertion if the gap exists.
        # (If APPROVAL_STALENESS_LIMIT_SECONDS <= 900, the gap doesn't exist
        # and the test cannot distinguish old from new threshold — skip.)
        if stale_limit_min <= 15:
            pytest.skip(
                f"APPROVAL_STALENESS_LIMIT_SECONDS={APPROVAL_STALENESS_LIMIT_SECONDS} "
                f"gives limit={stale_limit_min} min which is not > 15; "
                f"test requires limit > 15 to distinguish from old threshold"
            )

        now = datetime.now(timezone.utc)
        snapshot_ts = now - timedelta(minutes=gap_age_min)

        row = {
            "id":             1,
            "created_at":     now - timedelta(minutes=gap_age_min),
            "snapshot_id":    1,
            "symbol":         "IWM",
            "strategy":       "iron_condor",
            "score":          72.0,
            "account_id":     "PAPER",
            "gate_result":    "approved",
            "candidate_json": json.dumps({"net_credit": 0.65}),
            "llm_card":       json.dumps({"recommendation": "pass", "approval_status": "pending"}),
            "snapshot_ts":    snapshot_ts,
        }

        engine = _make_api_engine([row])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/candidates")

        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        # 16 min < 20 min limit → NOT stale with correct threshold
        assert body["candidates"][0]["is_stale"] is False
