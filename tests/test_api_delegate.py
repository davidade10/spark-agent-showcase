"""
tests/test_api_delegate.py — POST /candidates/{id}/delegate

Mocks DB + Telegram so no live Postgres or Telegram is required.

Manual checks (cannot be automated here):
  • Real Telegram delivery (test 1)
  • UI spinner / green flash / reset (test 2)
  • Double-click debounce — only one send (test 3) — enforced in TradeCard state
  • Invalid token → browser alert (test 4) — same as 502 path from API
  • Approve blocked while isDelegating (test 5) — frontend only
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from approval_ui.api import app


def _mock_row(data: dict):
    row = MagicMock()
    row._mapping = data
    return row


def _engine_delegate_fetchone(row_dict: dict | None):
    """Mock engine: connect() → execute → fetchone() returns _mock_row or None."""
    engine = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = _mock_row(row_dict) if row_dict is not None else None
    conn = MagicMock()
    conn.execute.return_value = result
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = conn
    begin_conn = MagicMock()
    begin_conn.__enter__ = MagicMock(return_value=begin_conn)
    begin_conn.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_conn
    return engine


@pytest.fixture
def client_with_migrations():
    with patch("approval_ui.api.migrate_orders_schema"), \
         patch("approval_ui.api.migrate_agent_config"):
        with TestClient(app) as c:
            yield c


class TestPostDelegate:
    CANDIDATE_ROW = {"symbol": "AAPL", "strategy": "IRON_CONDOR", "score": 71.0}

    def test_success_returns_true_and_calls_telegram(self, client_with_migrations):
        engine = _engine_delegate_fetchone(self.CANDIDATE_ROW)
        mock_send = MagicMock(return_value=True)
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.TELEGRAM_BOT_TOKEN", "test-token"), \
             patch("approval_ui.api.TELEGRAM_CHAT_ID", "999"), \
             patch("approval_ui.api.send_telegram_msg", mock_send):
            resp = client_with_migrations.post("/candidates/42/delegate")

        assert resp.status_code == 200
        assert resp.json() == {"success": True}
        mock_send.assert_called_once()
        msg = mock_send.call_args[0][0]
        assert "42" in msg and "AAPL" in msg and "IRON CONDOR" in msg.replace("_", " ")
        assert "71" in msg

    def test_not_found_404(self, client_with_migrations):
        engine = _engine_delegate_fetchone(None)
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.TELEGRAM_BOT_TOKEN", "t"), \
             patch("approval_ui.api.TELEGRAM_CHAT_ID", "1"), \
             patch("approval_ui.api.send_telegram_msg", return_value=True):
            resp = client_with_migrations.post("/candidates/999/delegate")

        assert resp.status_code == 404
        assert resp.json()["detail"] == "Candidate not found"

    def test_telegram_not_configured_500(self, client_with_migrations):
        engine = _engine_delegate_fetchone(self.CANDIDATE_ROW)
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.TELEGRAM_BOT_TOKEN", ""), \
             patch("approval_ui.api.TELEGRAM_CHAT_ID", ""), \
             patch("approval_ui.api.send_telegram_msg") as mock_send:
            resp = client_with_migrations.post("/candidates/1/delegate")

        assert resp.status_code == 500
        assert resp.json() == {"success": False, "error": "Telegram not configured"}
        mock_send.assert_not_called()

    def test_telegram_send_failed_502(self, client_with_migrations):
        engine = _engine_delegate_fetchone(self.CANDIDATE_ROW)
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.TELEGRAM_BOT_TOKEN", "t"), \
             patch("approval_ui.api.TELEGRAM_CHAT_ID", "1"), \
             patch("approval_ui.api.send_telegram_msg", return_value=False):
            resp = client_with_migrations.post("/candidates/1/delegate")

        assert resp.status_code == 502
        assert resp.json() == {"success": False, "error": "Telegram send failed"}

    def test_two_posts_invoke_telegram_twice(self, client_with_migrations):
        """Backend does not dedupe; UI debounce prevents double-click. Two POSTs = two calls."""
        engine = _engine_delegate_fetchone(self.CANDIDATE_ROW)
        mock_send = MagicMock(return_value=True)
        with patch("approval_ui.api.get_engine", return_value=engine), \
             patch("approval_ui.api.TELEGRAM_BOT_TOKEN", "t"), \
             patch("approval_ui.api.TELEGRAM_CHAT_ID", "1"), \
             patch("approval_ui.api.send_telegram_msg", mock_send):
            r1 = client_with_migrations.post("/candidates/5/delegate")
            r2 = client_with_migrations.post("/candidates/5/delegate")

        assert r1.status_code == 200 and r2.status_code == 200
        assert mock_send.call_count == 2
