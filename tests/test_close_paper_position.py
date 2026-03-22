"""
tests/test_close_paper_position.py

Tests for execution/close_paper_position.py.

Uses mock SQLAlchemy engines — no live database required.

execute() call sequence inside close_paper_position():
  1. SELECT position          (fetchone → pos_row or None)
  2. SELECT trade_decisions   (fetchone → decision row or None)  [skipped if no order_id]
  3. INSERT trade_outcomes    (fetchone → outcome row with .id)
  4. UPDATE positions         (return value not used)
  5. INSERT orders            (return value not used)

All 5 writes are in a single engine.begin() transaction.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call

from execution.close_paper_position import close_paper_position


# ── Mock-building helpers ──────────────────────────────────────────────────────

def _row(**kwargs):
    """Return a MagicMock that exposes attributes for the given kwargs."""
    r = MagicMock()
    for k, v in kwargs.items():
        setattr(r, k, v)
    return r


def _result(fetchone_val=None):
    """Wrap a fetchone return value in a mock execute-result object."""
    m = MagicMock()
    m.fetchone.return_value = fetchone_val
    return m


def _make_conn(side_effects: list):
    """
    Build a mock connection whose execute() returns/raises items in order.

    Each element in side_effects is either:
      - A mock result object (returned directly as the execute() return value)
      - An Exception instance  (raised when that call is made)
    """
    conn = MagicMock()
    conn.execute.side_effect = side_effects
    return conn


def _make_engine(conn):
    """Build a mock engine whose begin() context manager yields conn."""
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__  = MagicMock(return_value=False)
    return engine


# ── Standard fixture rows ──────────────────────────────────────────────────────

def _pos_row(**overrides):
    defaults = dict(
        id=50,
        account_id="PAPER",
        symbol="IWM",
        status="open",
        fill_credit=0.80,
        quantity=2,
        order_id=10,
    )
    defaults.update(overrides)
    return _row(**defaults)


def _decision_row(id=42):
    return _row(id=id)


def _outcome_row(id=99):
    return _row(id=id)


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestClosePaperPosition:

    def test_happy_path_close(self):
        """
        Full success path: valid PAPER position, decision traced, outcome written.
        Verifies return dict, P&L calculation, and that exactly 5 DB calls were made.
        """
        pos   = _pos_row()
        dec   = _decision_row(id=42)
        out   = _outcome_row(id=99)

        conn = _make_conn([
            _result(pos),   # 1. SELECT position
            _result(dec),   # 2. SELECT trade_decisions
            _result(out),   # 3. INSERT trade_outcomes RETURNING id
            _result(),      # 4. UPDATE positions
            _result(),      # 5. INSERT orders
        ])
        engine = _make_engine(conn)

        with patch("execution.close_paper_position.create_engine", return_value=engine):
            result = close_paper_position(50, 0.30, "PROFIT_TARGET")

        assert result["success"]          is True
        assert result["position_id"]      == 50
        # pnl = (0.80 - 0.30) * 2 * 100 = 100.00
        assert result["pnl"]              == pytest.approx(100.00)
        assert result["exit_debit"]       == pytest.approx(0.30)
        assert result["exit_reason"]      == "PROFIT_TARGET"
        assert result["trade_outcome_id"] == 99

        assert conn.execute.call_count == 5

    def test_pnl_loss_when_exit_debit_exceeds_fill_credit(self):
        """P&L is negative when buying back costs more than the credit received."""
        pos = _pos_row(fill_credit=0.50, quantity=1)
        conn = _make_conn([
            _result(pos),
            _result(_decision_row()),
            _result(_outcome_row()),
            _result(),
            _result(),
        ])
        engine = _make_engine(conn)

        with patch("execution.close_paper_position.create_engine", return_value=engine):
            result = close_paper_position(50, 1.50, "STOP_LOSS")

        # pnl = (0.50 - 1.50) * 1 * 100 = -100.00
        assert result["pnl"] == pytest.approx(-100.00)

    def test_fails_if_position_not_found(self):
        """ValueError raised when the position row does not exist."""
        conn = _make_conn([_result(None)])  # fetchone returns None
        engine = _make_engine(conn)

        with patch("execution.close_paper_position.create_engine", return_value=engine):
            with pytest.raises(ValueError, match="not found"):
                close_paper_position(999, 0.30)

        assert conn.execute.call_count == 1

    def test_fails_if_account_id_is_not_paper(self):
        """ValueError raised when the position belongs to a live account."""
        pos = _pos_row(account_id="8096")
        conn = _make_conn([_result(pos)])
        engine = _make_engine(conn)

        with patch("execution.close_paper_position.create_engine", return_value=engine):
            with pytest.raises(ValueError, match="PAPER"):
                close_paper_position(50, 0.30)

        assert conn.execute.call_count == 1

    def test_fails_if_account_is_roth_ira(self):
        """ValueError for the second live account too."""
        pos = _pos_row(account_id="5760")
        conn = _make_conn([_result(pos)])
        engine = _make_engine(conn)

        with patch("execution.close_paper_position.create_engine", return_value=engine):
            with pytest.raises(ValueError, match="PAPER"):
                close_paper_position(50, 0.30)

    def test_fails_if_position_already_closed(self):
        """ValueError raised when the position status is not 'open'."""
        pos = _pos_row(status="closed")
        conn = _make_conn([_result(pos)])
        engine = _make_engine(conn)

        with patch("execution.close_paper_position.create_engine", return_value=engine):
            with pytest.raises(ValueError, match="open"):
                close_paper_position(50, 0.30)

        assert conn.execute.call_count == 1

    def test_transaction_rollback_on_partial_failure(self):
        """
        If the INSERT into trade_outcomes raises, the exception propagates
        and the subsequent UPDATE + INSERT (positions, orders) are never reached.

        In production SQLAlchemy, engine.begin().__exit__ rolls back the
        transaction on exception. Here we verify the write count is exactly 3:
        position load, decision trace, failed INSERT — nothing after.
        """
        pos = _pos_row()
        dec = _decision_row()

        conn = _make_conn([
            _result(pos),                     # 1. SELECT position
            _result(dec),                     # 2. SELECT trade_decisions
            RuntimeError("DB constraint"),    # 3. INSERT trade_outcomes → raises
        ])
        engine = _make_engine(conn)

        with patch("execution.close_paper_position.create_engine", return_value=engine):
            with pytest.raises(RuntimeError, match="DB constraint"):
                close_paper_position(50, 0.30)

        # UPDATE positions (4) and INSERT orders (5) must NOT have been called
        assert conn.execute.call_count == 3

    def test_null_decision_id_when_no_order_id(self):
        """
        If the position has no order_id, decision_id is NULL in trade_outcomes.
        The decision-trace execute() call is skipped entirely (4 total calls).
        """
        pos = _pos_row(order_id=None)
        out = _outcome_row(id=77)

        conn = _make_conn([
            _result(pos),   # 1. SELECT position
            # call 2 (SELECT trade_decisions) is SKIPPED — no order_id
            _result(out),   # 2. INSERT trade_outcomes RETURNING id
            _result(),      # 3. UPDATE positions
            _result(),      # 4. INSERT orders
        ])
        engine = _make_engine(conn)

        with patch("execution.close_paper_position.create_engine", return_value=engine):
            result = close_paper_position(50, 0.40)

        assert result["success"]          is True
        assert result["trade_outcome_id"] == 77
        assert conn.execute.call_count    == 4

    def test_null_decision_id_when_trace_returns_nothing(self):
        """
        If order_id is set but the JOIN returns no matching trade_decisions row,
        decision_id is NULL. The close still succeeds (best-effort trace).
        """
        pos = _pos_row(order_id=10)
        out = _outcome_row(id=88)

        conn = _make_conn([
            _result(pos),    # 1. SELECT position
            _result(None),   # 2. SELECT trade_decisions → no match
            _result(out),    # 3. INSERT trade_outcomes RETURNING id
            _result(),       # 4. UPDATE positions
            _result(),       # 5. INSERT orders
        ])
        engine = _make_engine(conn)

        with patch("execution.close_paper_position.create_engine", return_value=engine):
            result = close_paper_position(50, 0.20, "TIME_EXIT")

        assert result["success"]          is True
        assert result["trade_outcome_id"] == 88
        assert conn.execute.call_count    == 5

    def test_default_exit_reason_is_manual(self):
        """exit_reason defaults to 'MANUAL' when not supplied."""
        pos = _pos_row()
        conn = _make_conn([
            _result(pos),
            _result(_decision_row()),
            _result(_outcome_row()),
            _result(),
            _result(),
        ])
        engine = _make_engine(conn)

        with patch("execution.close_paper_position.create_engine", return_value=engine):
            result = close_paper_position(50, 0.10)

        assert result["exit_reason"] == "MANUAL"
