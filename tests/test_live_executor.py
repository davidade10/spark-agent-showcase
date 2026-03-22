"""
tests/test_live_executor.py

Mocked tests for the live execution path in execution/executor.py and
live order tracking in execution/order_state.py.
No real Schwab orders are placed.  No real database connections are used.

Covers:
  - _read_live_account_flag: key present, missing, DB error, correct key name
  - _live_execution_guard: each of the three conditions raises RuntimeError
  - execute_approved_candidate (live path):
      - place_order called with correct account_hash and payload
      - raise_for_status called immediately
      - order ID extracted from Location header (not response.json())
      - orders table updated with source='live', status='submitted', schwab_order_id
      - NotImplementedError is no longer raised
  - get_live_order_status: FILLED, WORKING/partial, REJECTED, CANCELED, in-flight
  - confirm_live_fill: DB updates for each resolved status; partial fill does not close position
  - track_order (live): calls confirm_live_fill via schwab client
      - submission failure writes status='failed' + error_message and re-raises
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, call, patch

import pytest

from execution.executor import (
    _live_execution_guard,
    _read_live_account_flag,
)

# ── Shared test data ──────────────────────────────────────────────────────────

_CANDIDATE_JSON = {
    "symbol":             "NVDA",
    "expiry":             "2026-04-17",
    "long_put_strike":    130.0,
    "short_put_strike":   140.0,
    "short_call_strike":  210.0,
    "long_call_strike":   220.0,
    "net_credit":         2.50,
    "spread_width":       10.0,
}

_ACCOUNT_HASH = "FAKEHASH_ABCDE"
_SCHWAB_ORDER_ID = 98765


# ── Mock engine factory ───────────────────────────────────────────────────────

def _make_engine(order_id: int = 99):
    """
    Returns (engine, tx1_conn, tx2_conn).

    tx1_conn: handles SELECT trade_candidates + INSERT orders in one transaction.
    tx2_conn: handles the UPDATE orders (success or failure) in a second transaction.
    """
    candidate_row        = MagicMock()
    candidate_row.llm_card       = json.dumps({"approval_status": "approved"})
    candidate_row.candidate_json = json.dumps(_CANDIDATE_JSON)
    candidate_row.account_id     = "8096"
    candidate_row.symbol         = "NVDA"

    insert_result = MagicMock()
    insert_result.scalar.return_value = order_id

    tx1_conn = MagicMock()
    tx1_conn.execute.side_effect = [
        MagicMock(fetchone=MagicMock(return_value=candidate_row)),  # SELECT
        insert_result,                                               # INSERT RETURNING
    ]

    tx2_conn = MagicMock()

    def _ctx(conn):
        m = MagicMock()
        m.__enter__ = MagicMock(return_value=conn)
        m.__exit__  = MagicMock(return_value=False)
        return m

    engine = MagicMock()
    engine.begin.side_effect = [_ctx(tx1_conn), _ctx(tx2_conn)]
    return engine, tx1_conn, tx2_conn


def _live_patches(engine, mock_client, account_flag="true"):
    """Return the list of patch context managers for a live execution test."""
    return [
        patch("execution.executor.create_engine",           return_value=engine),
        patch("execution.executor.migrate_orders_schema"),
        patch("execution.executor.TRADING_MODE",            "live"),
        patch("execution.executor.ENABLE_LIVE_SEND",        True),
        # get_schwab_client is a lazy import inside the function body;
        # patch it at the source module so the import picks up the mock.
        patch("data_layer.provider.get_schwab_client",      return_value=mock_client),
        patch("execution.executor._resolve_account_hash",   return_value=_ACCOUNT_HASH),
        patch("execution.executor._read_live_account_flag", return_value=account_flag),
        patch("execution.executor._compute_quantity",       return_value=2),
    ]


def _mock_client(schwab_order_id=_SCHWAB_ORDER_ID):
    """Returns a mock Schwab client with a successful place_order response."""
    mock_utils_inst = MagicMock()
    mock_utils_inst.extract_order_id.return_value = schwab_order_id

    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None

    client = MagicMock()
    client.place_order.return_value = mock_resp
    return client, mock_resp, mock_utils_inst


# ── _read_live_account_flag ───────────────────────────────────────────────────

class TestReadLiveAccountFlag:

    def _engine_returning(self, db_value):
        """Mock engine whose agent_config query returns db_value (or None)."""
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (
            (db_value,) if db_value is not None else None
        )
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__  = MagicMock(return_value=False)
        engine = MagicMock()
        engine.connect.return_value = ctx
        return engine, conn

    def test_returns_true_when_key_is_true(self):
        engine, _ = self._engine_returning("true")
        with patch("execution.executor.create_engine", return_value=engine):
            assert _read_live_account_flag("8096") == "true"

    def test_returns_false_when_key_is_false(self):
        engine, _ = self._engine_returning("false")
        with patch("execution.executor.create_engine", return_value=engine):
            assert _read_live_account_flag("8096") == "false"

    def test_returns_false_when_key_missing(self):
        engine, _ = self._engine_returning(None)
        with patch("execution.executor.create_engine", return_value=engine):
            assert _read_live_account_flag("8096") == "false"

    def test_returns_false_on_db_error(self):
        engine = MagicMock()
        engine.connect.side_effect = Exception("connection refused")
        with patch("execution.executor.create_engine", return_value=engine):
            assert _read_live_account_flag("8096") == "false"

    def test_key_name_uses_account_suffix(self):
        """Query must use key='live_trading_enabled_{suffix}'."""
        engine, conn = self._engine_returning("true")
        with patch("execution.executor.create_engine", return_value=engine):
            _read_live_account_flag("5760")
        bound_params = conn.execute.call_args[0][1]
        assert bound_params["key"] == "live_trading_enabled_5760"


# ── _live_execution_guard ─────────────────────────────────────────────────────

class TestLiveExecutionGuard:

    def test_raises_when_trading_mode_not_live(self):
        with pytest.raises(RuntimeError, match="TRADING_MODE"):
            _live_execution_guard("paper", True, "8096")

    def test_error_contains_actual_mode_value(self):
        with pytest.raises(RuntimeError) as exc_info:
            _live_execution_guard("paper", True, "8096")
        assert "'paper'" in str(exc_info.value)

    def test_raises_when_enable_live_send_false(self):
        with pytest.raises(RuntimeError, match="ENABLE_LIVE_SEND"):
            _live_execution_guard("live", False, "8096")

    def test_raises_when_account_flag_false(self):
        with patch("execution.executor._read_live_account_flag", return_value="false"):
            with pytest.raises(RuntimeError, match="live_trading_enabled_8096"):
                _live_execution_guard("live", True, "8096")

    def test_raises_when_account_flag_missing(self):
        with patch("execution.executor._read_live_account_flag", return_value="false"):
            with pytest.raises(RuntimeError, match="live_trading_enabled_5760"):
                _live_execution_guard("live", True, "5760")

    def test_error_contains_account_suffix_and_flag_value(self):
        with patch("execution.executor._read_live_account_flag", return_value="false"):
            with pytest.raises(RuntimeError) as exc_info:
                _live_execution_guard("live", True, "8096")
        msg = str(exc_info.value)
        assert "8096" in msg
        assert "'false'" in msg

    def test_no_exception_when_all_conditions_met(self):
        with patch("execution.executor._read_live_account_flag", return_value="true"):
            _live_execution_guard("live", True, "8096")  # must not raise


# ── Live order submission path ────────────────────────────────────────────────

class TestLiveOrderSubmission:

    def test_place_order_called_with_account_hash_and_iron_condor_payload(self):
        engine, _, _ = _make_engine(order_id=99)
        client, mock_resp, mock_utils_inst = _mock_client()
        patches = _live_patches(engine, client)

        with patches[0], patches[1], patches[2], patches[3], patches[4], \
             patches[5], patches[6], patches[7], \
             patch("schwab.utils.Utils", return_value=mock_utils_inst):
            from execution.executor import execute_approved_candidate
            execute_approved_candidate(1)

        assert client.place_order.called
        account_hash_arg, payload_arg = client.place_order.call_args[0]
        assert account_hash_arg == _ACCOUNT_HASH
        assert payload_arg["complexOrderStrategyType"] == "IRON_CONDOR"
        assert payload_arg["orderType"] == "NET_CREDIT"
        assert len(payload_arg["orderLegCollection"]) == 4

    def test_raise_for_status_called_immediately_after_place_order(self):
        engine, _, _ = _make_engine()
        client, mock_resp, mock_utils_inst = _mock_client()
        patches = _live_patches(engine, client)

        with patches[0], patches[1], patches[2], patches[3], patches[4], \
             patches[5], patches[6], patches[7], \
             patch("schwab.utils.Utils", return_value=mock_utils_inst):
            from execution.executor import execute_approved_candidate
            execute_approved_candidate(1)

        mock_resp.raise_for_status.assert_called_once()

    def test_order_id_extracted_from_location_header_not_json(self):
        """extract_order_id must be called on the response; response.json() must NOT."""
        engine, _, _ = _make_engine()
        client, mock_resp, mock_utils_inst = _mock_client(schwab_order_id=11111)
        patches = _live_patches(engine, client)

        with patches[0], patches[1], patches[2], patches[3], patches[4], \
             patches[5], patches[6], patches[7], \
             patch("schwab.utils.Utils", return_value=mock_utils_inst):
            from execution.executor import execute_approved_candidate
            execute_approved_candidate(1)

        # extract_order_id called with the response object
        mock_utils_inst.extract_order_id.assert_called_once_with(mock_resp)
        # response.json() must NOT have been called
        mock_resp.json.assert_not_called()

    def test_orders_updated_with_live_source_submitted_status_schwab_id(self):
        engine, _, tx2_conn = _make_engine(order_id=77)
        client, mock_resp, mock_utils_inst = _mock_client(schwab_order_id=55555)
        patches = _live_patches(engine, client)

        with patches[0], patches[1], patches[2], patches[3], patches[4], \
             patches[5], patches[6], patches[7], \
             patch("schwab.utils.Utils", return_value=mock_utils_inst):
            from execution.executor import execute_approved_candidate
            execute_approved_candidate(1)

        assert tx2_conn.execute.called
        _, params = tx2_conn.execute.call_args[0]
        assert params["id"] == 77
        assert params["schwab_order_id"] == "55555"
        sql = str(tx2_conn.execute.call_args[0][0])
        assert "live" in sql
        assert "submitted" in sql

    def test_not_implemented_error_is_no_longer_raised(self):
        engine, _, _ = _make_engine()
        client, mock_resp, mock_utils_inst = _mock_client()
        patches = _live_patches(engine, client)

        with patches[0], patches[1], patches[2], patches[3], patches[4], \
             patches[5], patches[6], patches[7], \
             patch("schwab.utils.Utils", return_value=mock_utils_inst):
            from execution.executor import execute_approved_candidate
            try:
                execute_approved_candidate(1)
            except NotImplementedError:
                pytest.fail("NotImplementedError was raised — live send stub was not replaced")

    def test_submission_failure_writes_failed_status_and_reraises(self):
        engine, _, tx2_conn = _make_engine(order_id=88)
        client = MagicMock()
        client.place_order.side_effect = RuntimeError("Schwab 400 Bad Request")
        patches = _live_patches(engine, client)

        with patches[0], patches[1], patches[2], patches[3], patches[4], \
             patches[5], patches[6], patches[7]:
            from execution.executor import execute_approved_candidate
            with pytest.raises(RuntimeError, match="Schwab 400 Bad Request"):
                execute_approved_candidate(1)

        # Failure UPDATE must have been attempted
        assert tx2_conn.execute.called
        _, params = tx2_conn.execute.call_args[0]
        assert params["id"] == 88
        assert "Schwab 400 Bad Request" in params["err"]
        sql = str(tx2_conn.execute.call_args[0][0])
        assert "failed" in sql
        assert "error_message" in sql


# ── get_live_order_status ─────────────────────────────────────────────────────

from execution.order_state import (
    get_live_order_status,
    confirm_live_fill,
    FILLED, FAILED, CANCELLED, PARTIAL_FILL, SUBMITTED,
)


def _schwab_order_resp(status: str, filled_qty: float = 0, qty: float = 2,
                       price: float = 2.50) -> MagicMock:
    """Return a mock Schwab get_order response with the given status fields."""
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {
        "status":          status,
        "filledQuantity":  filled_qty,
        "quantity":        qty,
        "price":           price,
    }
    return resp


class TestGetLiveOrderStatus:

    def _client(self, status, filled_qty=0, qty=2):
        client = MagicMock()
        client.get_order.return_value = _schwab_order_resp(status, filled_qty, qty)
        return client

    def test_filled_returns_filled(self):
        assert get_live_order_status("1001", "HASH", self._client("FILLED")) == FILLED

    def test_rejected_returns_failed(self):
        assert get_live_order_status("1001", "HASH", self._client("REJECTED")) == FAILED

    def test_canceled_returns_cancelled(self):
        assert get_live_order_status("1001", "HASH", self._client("CANCELED")) == CANCELLED

    def test_cancelled_variant_returns_cancelled(self):
        assert get_live_order_status("1001", "HASH", self._client("CANCELLED")) == CANCELLED

    def test_expired_returns_cancelled(self):
        assert get_live_order_status("1001", "HASH", self._client("EXPIRED")) == CANCELLED

    def test_working_with_partial_fill_returns_partial_fill(self):
        client = self._client("WORKING", filled_qty=1, qty=2)
        assert get_live_order_status("1001", "HASH", client) == PARTIAL_FILL

    def test_working_with_no_fill_returns_submitted(self):
        client = self._client("WORKING", filled_qty=0, qty=2)
        assert get_live_order_status("1001", "HASH", client) == SUBMITTED

    def test_queued_returns_submitted(self):
        assert get_live_order_status("1001", "HASH", self._client("QUEUED")) == SUBMITTED

    def test_raise_for_status_called(self):
        client = MagicMock()
        mock_resp = _schwab_order_resp("FILLED")
        client.get_order.return_value = mock_resp
        get_live_order_status("1001", "HASH", client)
        mock_resp.raise_for_status.assert_called_once()


# ── confirm_live_fill ─────────────────────────────────────────────────────────

def _mock_order_engine(schwab_order_id="99001", account_id="8096",
                       symbol="NVDA", qty=2):
    """Return (engine, select_conn, update_conn) for confirm_live_fill tests."""
    order_row = MagicMock()
    order_row.schwab_order_id = schwab_order_id
    order_row.account_id      = account_id
    order_row.symbol          = symbol
    order_row.quantity        = qty

    select_conn = MagicMock()
    select_conn.execute.return_value.fetchone.return_value = order_row

    update_conn = MagicMock()

    def _ctx(conn):
        m = MagicMock()
        m.__enter__ = MagicMock(return_value=conn)
        m.__exit__  = MagicMock(return_value=False)
        return m

    engine = MagicMock()
    engine.connect.return_value   = _ctx(select_conn)
    engine.begin.return_value     = _ctx(update_conn)
    return engine, select_conn, update_conn


def _confirm_client(status, filled_qty=0, qty=2, price=2.50):
    """Return a mock client ready for confirm_live_fill."""
    client = MagicMock()
    # get_account_numbers for hash resolution
    client.get_account_numbers.return_value.raise_for_status.return_value = None
    client.get_account_numbers.return_value.json.return_value = [
        {"accountNumber": "xxxxx8096", "hashValue": "FAKEHASH"},
    ]
    # get_order for status poll
    client.get_order.return_value = _schwab_order_resp(status, filled_qty, qty, price)
    return client


class TestConfirmLiveFill:

    def test_filled_updates_status_fill_price_and_filled_at(self):
        engine, _, update_conn = _mock_order_engine()
        client = _confirm_client("FILLED", price=2.50)
        with patch("execution.order_state.create_engine", return_value=engine):
            result = confirm_live_fill(1, client)
        assert result == FILLED
        _, params = update_conn.execute.call_args[0]
        assert params["fill_price"] == 2.50
        assert "now" in params
        sql = str(update_conn.execute.call_args[0][0])
        assert "filled" in sql
        assert "fill_price" in sql

    def test_partial_fill_sets_partial_fill_status_and_does_not_close_position(self):
        engine, _, update_conn = _mock_order_engine()
        client = _confirm_client("WORKING", filled_qty=1, qty=2)
        with patch("execution.order_state.create_engine", return_value=engine):
            result = confirm_live_fill(1, client)
        assert result == PARTIAL_FILL
        sql = str(update_conn.execute.call_args[0][0])
        assert "partial_fill" in sql
        # No fill_price or filled_at — position stays open
        _, params = update_conn.execute.call_args[0]
        assert "fill_price" not in params

    def test_cancelled_updates_status_to_cancelled(self):
        engine, _, update_conn = _mock_order_engine()
        client = _confirm_client("CANCELED")
        with patch("execution.order_state.create_engine", return_value=engine):
            result = confirm_live_fill(1, client)
        assert result == CANCELLED
        _, params = update_conn.execute.call_args[0]
        assert params["status"] == CANCELLED

    def test_rejected_updates_status_to_failed(self):
        engine, _, update_conn = _mock_order_engine()
        client = _confirm_client("REJECTED")
        with patch("execution.order_state.create_engine", return_value=engine):
            result = confirm_live_fill(1, client)
        assert result == FAILED
        _, params = update_conn.execute.call_args[0]
        assert params["status"] == FAILED

    def test_in_flight_makes_no_db_write(self):
        engine, _, update_conn = _mock_order_engine()
        client = _confirm_client("WORKING", filled_qty=0)
        with patch("execution.order_state.create_engine", return_value=engine):
            result = confirm_live_fill(1, client)
        assert result == SUBMITTED
        update_conn.execute.assert_not_called()

    def test_raises_value_error_when_order_not_found(self):
        engine = MagicMock()
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=MagicMock(
            **{"execute.return_value.fetchone.return_value": None}
        ))
        ctx.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value = ctx
        with patch("execution.order_state.create_engine", return_value=engine):
            with pytest.raises(ValueError, match="not found"):
                confirm_live_fill(999, MagicMock())

    def test_raises_value_error_when_schwab_order_id_missing(self):
        engine, _, _ = _mock_order_engine(schwab_order_id=None)
        with patch("execution.order_state.create_engine", return_value=engine):
            with pytest.raises(ValueError, match="schwab_order_id"):
                confirm_live_fill(1, MagicMock())


# ── track_order live path ─────────────────────────────────────────────────────

class TestTrackOrderLivePath:

    def test_live_mode_calls_confirm_live_fill(self):
        mock_client = MagicMock()
        with patch("execution.order_state.TRADING_MODE", "live"), \
             patch("data_layer.provider.get_schwab_client", return_value=mock_client), \
             patch("execution.order_state.confirm_live_fill") as mock_confirm:
            from execution.order_state import track_order
            track_order(42)
        mock_confirm.assert_called_once_with(42, mock_client)

    def test_live_mode_no_longer_raises_not_implemented(self):
        mock_client = MagicMock()
        with patch("execution.order_state.TRADING_MODE", "live"), \
             patch("data_layer.provider.get_schwab_client", return_value=mock_client), \
             patch("execution.order_state.confirm_live_fill"):
            from execution.order_state import track_order
            try:
                track_order(42)
            except NotImplementedError:
                pytest.fail("NotImplementedError was raised — live tracking stub not replaced")
