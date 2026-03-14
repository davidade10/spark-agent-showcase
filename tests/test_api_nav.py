"""
tests/test_api_nav.py — Tests for the GET /nav endpoint.

The /nav endpoint reads the last non-empty line of reconciler.log (a
newline-delimited JSON file written by run_scheduled_reconciliation).
No database is needed.  Tests patch RECONCILER_LOG to a tmp_path file.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from approval_ui.api import app


# ── Fixture ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def client():
    """TestClient with startup migration suppressed."""
    with patch("approval_ui.api.migrate_orders_schema"), \
         patch("approval_ui.api.get_engine", return_value=_no_op_engine()):
        with TestClient(app) as c:
            yield c


def _no_op_engine():
    from unittest.mock import MagicMock
    engine = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=MagicMock())
    ctx.__exit__  = MagicMock(return_value=False)
    engine.begin.return_value   = ctx
    engine.connect.return_value = ctx
    return engine


def _log_entry(combined: float, accounts: dict | None = None) -> str:
    return json.dumps({
        "ts":  "2026-03-14T10:00:00+00:00",
        "nav": {
            "combined_live_nav": combined,
            "accounts": accounts or {},
        },
    })


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestGetNav:
    def test_missing_log_returns_zero(self, client):
        with patch("approval_ui.api.RECONCILER_LOG",
                   Path("/nonexistent/path/reconciler.log")):
            resp = client.get("/nav")
        assert resp.status_code == 200
        assert resp.json()["combined_live_nav"] == 0

    def test_valid_log_returns_combined_nav(self, client, tmp_path):
        log = tmp_path / "reconciler.log"
        log.write_text(
            _log_entry(14813.46, {"5760": 6621.12, "8096": 8192.34}) + "\n"
        )
        with patch("approval_ui.api.RECONCILER_LOG", log):
            resp = client.get("/nav")

        body = resp.json()
        assert body["combined_live_nav"] == pytest.approx(14813.46, abs=0.01)
        assert body["accounts"]["5760"]  == pytest.approx(6621.12,  abs=0.01)
        assert body["accounts"]["8096"]  == pytest.approx(8192.34,  abs=0.01)

    def test_last_line_wins_with_multiple_entries(self, client, tmp_path):
        """Only the last non-empty line is parsed."""
        log = tmp_path / "reconciler.log"
        log.write_text(
            _log_entry(1000.00) + "\n" +
            _log_entry(9999.99) + "\n"
        )
        with patch("approval_ui.api.RECONCILER_LOG", log):
            resp = client.get("/nav")
        assert resp.json()["combined_live_nav"] == pytest.approx(9999.99, abs=0.01)

    def test_empty_log_returns_zero(self, client, tmp_path):
        log = tmp_path / "reconciler.log"
        log.write_text("")
        with patch("approval_ui.api.RECONCILER_LOG", log):
            resp = client.get("/nav")
        assert resp.json()["combined_live_nav"] == 0

    def test_malformed_json_returns_zero(self, client, tmp_path):
        log = tmp_path / "reconciler.log"
        log.write_text("not valid json\n")
        with patch("approval_ui.api.RECONCILER_LOG", log):
            resp = client.get("/nav")
        assert resp.json()["combined_live_nav"] == 0

    def test_log_missing_nav_key_returns_zero(self, client, tmp_path):
        """A valid JSON line that lacks the 'nav' key should fall back to 0."""
        log = tmp_path / "reconciler.log"
        log.write_text(json.dumps({"ts": "2026-03-14", "positions": {}}) + "\n")
        with patch("approval_ui.api.RECONCILER_LOG", log):
            resp = client.get("/nav")
        assert resp.json()["combined_live_nav"] == 0
