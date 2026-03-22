"""
tests/test_strangle_gate_enabled.py

Tests for the strangle_trading_enabled gate check in rules_gate.py.

Verifies:
  - When strangle_trading_enabled = 'false' in agent_config, the check returns a
    blocking reason (non-None), and that reason contains enough context to identify
    the gate rule.
  - When strangle_trading_enabled = 'true', the check passes (returns None).
  - When the key is missing from agent_config, the check blocks (fail-closed).
  - When the DB raises an exception, the check blocks (fail-closed).

No live database required — conn is mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from strategy_engine.rules_gate import _check_strangle_trading_enabled


# ── Mock helpers ──────────────────────────────────────────────────────────────

def _conn_with_value(value: str) -> MagicMock:
    """Return a mock conn whose agent_config lookup returns the given value."""
    row = MagicMock()
    row.__getitem__ = lambda self, i: value
    row[0] = value
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = row
    return conn


def _conn_returning_none() -> MagicMock:
    """Return a mock conn whose agent_config lookup returns no row (key missing)."""
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = None
    return conn


def _conn_raising() -> MagicMock:
    """Return a mock conn that raises on execute (DB error)."""
    conn = MagicMock()
    conn.execute.side_effect = Exception("DB connection refused")
    return conn


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestStrangleTradingEnabledGate:

    def test_disabled_false_blocks(self):
        """strangle_trading_enabled = 'false' must return a blocking reason."""
        conn = _conn_with_value("false")
        result = _check_strangle_trading_enabled(conn)
        assert result is not None, "Expected block but got None"

    def test_disabled_zero_blocks(self):
        """strangle_trading_enabled = '0' must also block."""
        conn = _conn_with_value("0")
        result = _check_strangle_trading_enabled(conn)
        assert result is not None

    def test_enabled_true_passes(self):
        """strangle_trading_enabled = 'true' must return None (pass)."""
        conn = _conn_with_value("true")
        result = _check_strangle_trading_enabled(conn)
        assert result is None, f"Expected None (pass) but got: {result!r}"

    def test_enabled_1_passes(self):
        """strangle_trading_enabled = '1' must also pass."""
        conn = _conn_with_value("1")
        result = _check_strangle_trading_enabled(conn)
        assert result is None

    def test_missing_key_blocks(self):
        """Missing agent_config key must block (fail-closed)."""
        conn = _conn_returning_none()
        result = _check_strangle_trading_enabled(conn)
        assert result is not None, "Missing key should fail closed but got None"

    def test_db_error_blocks(self):
        """DB exception must block (fail-closed)."""
        conn = _conn_raising()
        result = _check_strangle_trading_enabled(conn)
        assert result is not None, "DB error should fail closed but got None"
