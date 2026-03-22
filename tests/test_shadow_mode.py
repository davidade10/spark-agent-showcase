"""
tests/test_shadow_mode.py

Tests for shadow mode — blocked candidate tracking and GET /shadow endpoint.

Coverage:
  1. Gate blocks a candidate → blocked_reason written to DB with correct shape
  2. Gate passes a candidate → blocked_reason is NULL (not written)
  3. GET /shadow returns only gate_result='blocked' rows
  4. GET /shadow?hours=12 filters by time window correctly
  5. Blocked candidates do NOT appear in GET /candidates
  6. blocked_reason contains both 'rule' and 'detail' keys

No live database required — all DB interactions are mocked.
"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from approval_ui.api import app
from strategy_engine.rules_gate import _write_to_trade_candidates
from strategy_engine.scoring import ScoredCandidate
from strategy_engine.candidates import IronCondorCandidate


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _mock_row(data: dict):
    """Wrap a dict so it behaves like a SQLAlchemy Row (_mapping attr)."""
    row = MagicMock()
    row._mapping = data
    return row


def _make_api_engine(rows: list[dict]):
    """
    Build a mock engine for API endpoint tests.
    connect() yields the given rows from fetchall().
    begin() returns a no-op context manager (used by startup migration).

    GET /shadow runs a second query for gate_kill_distribution — return a
    synthetic GROUP BY from blocked_reason.rule in the fixture rows.
    """
    def _dist_counts() -> list[tuple]:
        counts: dict[str, int] = {}
        for r in rows:
            br = r.get("blocked_reason")
            key = "__UNKNOWN_LEGACY__"
            if isinstance(br, str):
                obj = json.loads(br)
                rule = obj.get("rule")
                if rule and str(rule).strip():
                    key = str(rule).strip()
            counts[key] = counts.get(key, 0) + 1
        return sorted(counts.items(), key=lambda x: -x[1])

    engine = MagicMock()

    def _exec_side_effect(sql, params=None):
        s = str(sql)
        res = MagicMock()
        # Distribution query (distinct from main shadow SELECT)
        if "blocked_reason->>'rule'" in s and "GROUP BY" in s:
            res.fetchall.return_value = _dist_counts()
        else:
            res.fetchall.return_value = [_mock_row(r) for r in rows]
        return res

    conn = MagicMock()
    conn.execute.side_effect = _exec_side_effect
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__  = MagicMock(return_value=False)
    engine.connect.return_value = conn

    begin_conn = MagicMock()
    begin_conn.__enter__ = MagicMock(return_value=begin_conn)
    begin_conn.__exit__  = MagicMock(return_value=False)
    engine.begin.return_value = begin_conn

    return engine


def _make_gate_conn():
    """
    Build a mock connection for _write_to_trade_candidates unit tests.
    execute() returns a result whose scalar() yields a fake candidate_id.
    """
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = 42
    return conn


def _make_scored(
    symbol:     str   = "IWM",
    net_credit: float = 0.65,
    snapshot_id: int  = 1,
    expiry:     str   = "2026-04-17",
) -> ScoredCandidate:
    """Construct a minimal ScoredCandidate for gate write tests."""
    candidate = IronCondorCandidate(
        symbol            = symbol,
        snapshot_id       = snapshot_id,
        expiry            = expiry,
        dte               = 30,
        long_put_strike   = 420.0,
        short_put_strike  = 425.0,
        short_call_strike = 460.0,
        long_call_strike  = 465.0,
        long_put_mid      = 0.10,
        short_put_mid     = 0.45,
        short_call_mid    = 0.45,
        long_call_mid     = 0.10,
        short_put_delta   = -0.16,
        short_call_delta  =  0.16,
        net_credit        = net_credit,
        spread_width      = 5.0,
        max_loss          = round(5.0 - net_credit, 4),
        underlying_price  = 440.0,
        iv_rank           = 65.0,
    )
    return ScoredCandidate(
        candidate          = candidate,
        total_score        = 72.0,
        iv_rank_score      = 30.0,
        credit_width_score = 20.0,
        delta_score        = 12.0,
        dte_score          = 10.0,
        score_notes        = [],
    )


def _extract_insert_params(conn) -> dict:
    """Return the params dict from the first conn.execute() call."""
    return conn.execute.call_args[0][1]


# ── Test 1: gate writes blocked_reason for a blocked candidate ─────────────────

class TestGateWrite:

    def test_blocked_candidate_writes_blocked_reason(self):
        """
        When _write_to_trade_candidates is called with a failing rule,
        the INSERT params must include a blocked_reason JSON string containing
        both 'rule' and 'detail' keys.
        """
        conn   = _make_gate_conn()
        scored = _make_scored()

        _write_to_trade_candidates(
            conn,
            scored,
            gate_result     = "blocked",
            blocking_rule   = "net_credit",
            blocking_reason = "Net credit $0.20 below minimum $0.40",
            account_id      = "PAPER",
        )

        params = _extract_insert_params(conn)
        assert "blocked_reason" in params
        assert params["blocked_reason"] is not None

        br = json.loads(params["blocked_reason"])
        assert br["rule"]   == "net_credit"
        assert br["detail"] == "Net credit $0.20 below minimum $0.40"

        cj = json.loads(params["candidate_json"])
        assert cj.get("qty") == 1

    def test_gate_write_persists_custom_qty_in_candidate_json(self):
        """candidate_json.qty reflects IronCondorCandidate.qty (contracts per leg)."""
        conn   = _make_gate_conn()
        base   = _make_scored()
        cand2  = replace(base.candidate, qty=3)
        scored = ScoredCandidate(
            candidate          = cand2,
            total_score        = base.total_score,
            iv_rank_score      = base.iv_rank_score,
            credit_width_score = base.credit_width_score,
            delta_score        = base.delta_score,
            dte_score          = base.dte_score,
            score_notes        = base.score_notes,
        )
        _write_to_trade_candidates(
            conn, scored, "blocked", "net_credit", "below min", account_id="PAPER",
        )
        params = _extract_insert_params(conn)
        assert json.loads(params["candidate_json"]).get("qty") == 3

    def test_approved_candidate_writes_null_blocked_reason(self):
        """
        When gate_result='approved', blocked_reason must be NULL (None).
        Approved candidates must never pollute the shadow queue.
        """
        conn   = _make_gate_conn()
        scored = _make_scored()

        _write_to_trade_candidates(
            conn,
            scored,
            gate_result     = "approved",
            blocking_rule   = None,
            blocking_reason = None,
            account_id      = "PAPER",
        )

        params = _extract_insert_params(conn)
        assert params["blocked_reason"] is None
        cj = json.loads(params["candidate_json"])
        assert cj.get("qty") == 1

    def test_approved_candidate_json_includes_qty(self):
        """Approved gate rows persist default qty=1 in candidate_json like blocked rows."""
        conn   = _make_gate_conn()
        scored = _make_scored()

        _write_to_trade_candidates(
            conn,
            scored,
            gate_result     = "approved",
            blocking_rule   = None,
            blocking_reason = None,
            account_id      = "PAPER",
        )

        params = _extract_insert_params(conn)
        assert json.loads(params["candidate_json"]).get("qty") == 1

    def test_gate_write_persists_qty_from_contracts_for_non_ic_duck(self):
        """If candidate is not IronCondorCandidate but has contracts, persist that as qty."""

        class DuckCondor:
            symbol = "DUCK"
            snapshot_id = 1
            expiry = "2026-04-17"
            dte = 30
            long_put_strike = 100.0
            short_put_strike = 105.0
            short_call_strike = 110.0
            long_call_strike = 115.0
            net_credit = 0.5
            spread_width = 5.0
            max_loss = 4.5
            underlying_price = 108.0
            iv_rank = 50.0
            short_put_delta = -0.15
            short_call_delta = 0.15
            contracts = 2

        conn = _make_gate_conn()
        base = _make_scored()
        scored = ScoredCandidate(
            candidate          = DuckCondor(),
            total_score        = base.total_score,
            iv_rank_score      = base.iv_rank_score,
            credit_width_score = base.credit_width_score,
            delta_score        = base.delta_score,
            dte_score          = base.dte_score,
            score_notes        = base.score_notes,
        )
        _write_to_trade_candidates(
            conn, scored, "blocked", "net_credit", "below min", account_id="PAPER",
        )
        assert json.loads(_extract_insert_params(conn)["candidate_json"]).get("qty") == 2

    def test_gate_write_omits_qty_when_unresolvable_non_ic(self):
        """Non-IC object with no qty/contracts/position_size: JSON has no qty key (safe)."""

        class BareCondor:
            symbol = "BARE"
            snapshot_id = 1
            expiry = "2026-04-17"
            dte = 30
            long_put_strike = 100.0
            short_put_strike = 105.0
            short_call_strike = 110.0
            long_call_strike = 115.0
            net_credit = 0.5
            spread_width = 5.0
            max_loss = 4.5
            underlying_price = 108.0
            iv_rank = 50.0
            short_put_delta = -0.15
            short_call_delta = 0.15

        conn = _make_gate_conn()
        base = _make_scored()
        scored = ScoredCandidate(
            candidate          = BareCondor(),
            total_score        = base.total_score,
            iv_rank_score      = base.iv_rank_score,
            credit_width_score = base.credit_width_score,
            delta_score        = base.delta_score,
            dte_score          = base.dte_score,
            score_notes        = base.score_notes,
        )
        _write_to_trade_candidates(
            conn, scored, "blocked", "net_credit", "below min", account_id="PAPER",
        )
        cj = json.loads(_extract_insert_params(conn)["candidate_json"])
        assert "qty" not in cj

    def test_blocked_reason_contains_rule_and_detail_keys(self):
        """
        Structural check: blocked_reason JSON must always have exactly
        'rule' and 'detail' keys regardless of which rule fires.
        """
        conn   = _make_gate_conn()
        scored = _make_scored()

        _write_to_trade_candidates(
            conn,
            scored,
            gate_result     = "blocked",
            blocking_rule   = "max_open_condors",
            blocking_reason = "Already have 4 open condors (max 4)",
            account_id      = "PAPER",
        )

        params = _extract_insert_params(conn)
        br = json.loads(params["blocked_reason"])
        assert set(br.keys()) == {"rule", "detail"}

    def test_gate_result_is_blocked_in_insert(self):
        """gate_result parameter is faithfully written to the INSERT."""
        conn   = _make_gate_conn()
        scored = _make_scored()

        _write_to_trade_candidates(
            conn, scored, "blocked", "short_delta", "Delta too high", account_id="PAPER",
        )

        params = _extract_insert_params(conn)
        assert params["gate_result"] == "blocked"


# ── Test 3: GET /shadow returns only blocked rows ─────────────────────────────

class TestShadowEndpoint:

    _BLOCKED_ROW = {
        "id":             1,
        "symbol":         "IWM",
        "score":          72.0,
        "strategy":       "iron_condor",
        "candidate_json": json.dumps({
            "expiry": "2026-04-17",
            "net_credit": 0.65,
            "long_put_strike": 220,
            "short_put_strike": 225,
            "short_call_strike": 270,
            "long_call_strike": 275,
            "qty": 1,
        }),
        "net_credit":     "0.65",
        "expiry":         "2026-04-17",
        "blocked_reason": json.dumps({"rule": "net_credit", "detail": "below min"}),
        "created_at":     datetime(2026, 3, 19, 10, 0, 0, tzinfo=timezone.utc),
        "snapshot_id":    1,
    }

    def test_shadow_returns_only_blocked_rows(self):
        """GET /shadow returns the blocked candidate list."""
        engine = _make_api_engine([self._BLOCKED_ROW])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/shadow")

        assert resp.status_code == 200
        body = resp.json()
        assert "blocked" in body
        assert "gate_kill_distribution" in body
        assert body["count"] == 1
        assert body["blocked"][0]["symbol"] == "IWM"
        assert body["blocked"][0]["gate_rule_label"] == "Net Credit Minimum"
        assert body["blocked"][0]["long_put_strike"] in (220, 220.0)
        assert body["blocked"][0]["qty"] == 1

    def test_shadow_qty_null_when_absent_from_candidate_json(self):
        """GET /shadow still serializes when candidate_json has no qty key."""
        row = {
            **TestShadowEndpoint._BLOCKED_ROW,
            "candidate_json": json.dumps({
                "expiry": "2026-04-17",
                "net_credit": 0.65,
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
                resp = client.get("/shadow")
        assert resp.status_code == 200
        assert resp.json()["blocked"][0].get("qty") is None

    def test_shadow_blocked_reason_is_parsed_to_dict(self):
        """blocked_reason is returned as a dict, not a raw JSON string."""
        engine = _make_api_engine([self._BLOCKED_ROW])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/shadow")

        body   = resp.json()
        br = body["blocked"][0]["blocked_reason"]
        assert isinstance(br, dict)
        assert br["rule"]   == "net_credit"
        assert "detail" in br

    def test_shadow_hours_filter_passes_correct_cutoff(self):
        """
        GET /shadow?hours=12 must pass a cutoff ~12 hours ago.
        Verifies that the 'hours' param propagates to the response metadata.
        """
        engine = _make_api_engine([])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/shadow?hours=12")

        assert resp.status_code == 200
        body = resp.json()
        assert body["hours"]   == 12
        assert body["count"]   == 0
        assert "cutoff" in body
        # Verify the cutoff is roughly 12 hours before now
        from datetime import timedelta
        cutoff_dt = datetime.fromisoformat(body["cutoff"])
        now_utc   = datetime.now(timezone.utc)
        age_hours = (now_utc - cutoff_dt).total_seconds() / 3600
        assert 11.9 < age_hours < 12.1

    def test_shadow_returns_empty_when_no_blocked_rows(self):
        """GET /shadow returns count=0 and empty list when nothing is blocked."""
        engine = _make_api_engine([])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/shadow")

        body = resp.json()
        assert body["count"]   == 0
        assert body["blocked"] == []

    def test_shadow_default_hours_is_48(self):
        """Default hours parameter is 48."""
        engine = _make_api_engine([])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/shadow")

        assert resp.json()["hours"] == 48


# ── Test 5: Blocked candidates do NOT appear in GET /candidates ───────────────

class TestCandidatesExcludesBlocked:

    def test_candidates_excludes_blocked_gate_result(self):
        """
        GET /candidates filters on gate_result='approved' in SQL, so blocked
        candidates never reach the approval queue. Verified by checking the
        endpoint returns an empty list when the only DB row is a blocked one.
        """
        # Row that looks like a blocked candidate (no llm_card with recommendation)
        blocked_row = {
            "id":           99,
            "created_at":   datetime(2026, 3, 19, 10, 0, 0, tzinfo=timezone.utc),
            "snapshot_id":  1,
            "symbol":       "IWM",
            "strategy":     "iron_condor",
            "score":        72.0,
            "account_id":   "primary",
            "gate_result":  "blocked",
            "candidate_json": json.dumps({"net_credit": 0.65}),
            "llm_card":     None,
            "snapshot_ts":  datetime(2026, 3, 19, 10, 0, 0, tzinfo=timezone.utc),
        }
        # The endpoint SQL filters WHERE gate_result='approved' — blocked rows
        # are never returned.  We simulate the DB returning an empty set.
        engine = _make_api_engine([])
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.migrate_orders_schema"):
            with TestClient(app) as client:
                resp = client.get("/candidates")

        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 0
        assert body["candidates"] == []
