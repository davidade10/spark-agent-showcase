"""
tests/test_closure_safety.py — Tests for false-closure prevention.

Covers two independent safety systems:

1. Parser health check — reconcile() blocks ALL closure writes when the
   ratio of recognized option legs falls below 0.5, or when zero legs are
   recognized from a non-empty batch. Tested by verifying that
   _parse_schwab_positions returns correct `legs` counts so the arithmetic
   in reconcile() produces the right threshold decision.

2. 3-strike system — a DB position absent from one Schwab snapshot is NOT
   closed immediately.  It accumulates a strike (closure_strikes++) and is
   only closed after three consecutive absences.  Tested via reconcile()
   with a minimal mock engine + mock Schwab client so no live DB is needed.

3. _match_position — pure function; tested for all strategy types so we
   know the "matched" branch fires correctly and the "absent" (closure)
   branch only fires when it should.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, call, patch

from tests.conftest import (
    make_option_position,
    make_equity_position,
    make_iron_condor_positions,
)
from data_layer.reconciler import _parse_schwab_positions, _match_position


ACCOUNT    = "5760"
EXPIRY     = "260417"
EXPIRY_ISO = "2026-04-17"


# ── Helper: build a minimal mock engine ───────────────────────────────────────

def _make_mock_engine(db_position_dicts: list[dict]):
    """
    Build a SQLAlchemy engine mock suitable for reconcile().

    engine.begin() is called twice:
      call 0 — run_count SELECT + INSERT (reconciler_state)
      call 1 — all reconcile writes (INSERT / UPDATE / closure)

    engine.connect() is called once to load open DB positions.

    Returns (engine, write_calls) where write_calls is a list that accumulates
    (sql_text, params) for every execute() on the second begin() connection.
    """
    engine = MagicMock()

    # ── Shared write-call log ─────────────────────────────────────────────────
    write_calls: list[tuple[str, dict]] = []

    # ── Connection for run-counter (engine.begin() call 0) ────────────────────
    rc_conn = MagicMock()
    rc_result = MagicMock()
    rc_result.fetchone.return_value = None          # no existing run_count row
    rc_conn.execute.return_value = rc_result
    rc_ctx = MagicMock()
    rc_ctx.__enter__ = MagicMock(return_value=rc_conn)
    rc_ctx.__exit__  = MagicMock(return_value=False)

    # ── Connection for reconcile writes (engine.begin() call 1) ──────────────
    wr_conn = MagicMock()

    def _wr_execute(sql, params=None):
        write_calls.append((str(sql), params or {}))
        return MagicMock()

    wr_conn.execute.side_effect = _wr_execute
    wr_ctx = MagicMock()
    wr_ctx.__enter__ = MagicMock(return_value=wr_conn)
    wr_ctx.__exit__  = MagicMock(return_value=False)

    # Alternate between the two on successive begin() calls
    _begin_count = [0]

    def _begin_side_effect():
        n = _begin_count[0]
        _begin_count[0] += 1
        return rc_ctx if n == 0 else wr_ctx

    engine.begin.side_effect = _begin_side_effect

    # ── Read connection for loading DB positions ──────────────────────────────
    db_rows = []
    for d in db_position_dicts:
        row = MagicMock()
        row._mapping = d
        db_rows.append(row)

    rd_result = MagicMock()
    rd_result.fetchall.return_value = db_rows
    rd_conn = MagicMock()
    rd_conn.execute.return_value = rd_result
    rd_ctx = MagicMock()
    rd_ctx.__enter__ = MagicMock(return_value=rd_conn)
    rd_ctx.__exit__  = MagicMock(return_value=False)
    engine.connect.return_value = rd_ctx

    return engine, write_calls


def _make_schwab_client(positions: list[dict], last4: str = "5760"):
    """Build a mock Schwab client returning a single account with given positions."""
    client = MagicMock()

    acct_resp = MagicMock()
    acct_resp.json.return_value = [
        {"accountNumber": f"123{last4}", "hashValue": f"hash_{last4}"}
    ]
    client.get_account_numbers.return_value = acct_resp

    pos_resp = MagicMock()
    pos_resp.json.return_value = {
        "securitiesAccount": {"positions": positions}
    }
    client.get_account.return_value = pos_resp
    # Attribute access for Fields enum
    client.Account.Fields.POSITIONS = "positions"

    return client


# ── Parser health-check arithmetic ────────────────────────────────────────────

class TestParserHealthCheckArithmetic:
    """
    Verify that _parse_schwab_positions returns the legs counts that make
    reconcile()'s health-check arithmetic correct.

    reconcile() computes:
        recognized_option_legs = condors*4 + sum(nc.legs for nc in non_condors
                                                  if nc.strategy != 'IRON_CONDOR')
        skipped_legs = max(0, total_legs_received - recognized_option_legs)
        parse_ratio  = (total_legs_received - skipped_legs) / total_legs_received
        closures_blocked if recognized_total==0 or parse_ratio < 0.5
    """

    def test_one_condor_contributes_4_recognized_legs(self):
        positions = make_iron_condor_positions("SPY", EXPIRY, 530, 535, 560, 565)
        condors, non_condors = _parse_schwab_positions(positions, ACCOUNT, [])
        recognized = len(condors) * 4 + sum(p["legs"] for p in non_condors)
        assert recognized == 4

    def test_short_option_contributes_1_recognized_leg(self):
        pos = make_option_position("SMCI", EXPIRY, "C", 40.0, short_qty=1)
        _, non_condors = _parse_schwab_positions([pos], ACCOUNT, [])
        assert non_condors[0]["legs"] == 1

    def test_condor_plus_short_option_5_recognized_of_5(self):
        condor_legs = make_iron_condor_positions("SPY", EXPIRY, 530, 535, 560, 565)
        short_call  = make_option_position("SMCI", EXPIRY, "C", 40.0, short_qty=1)
        condors, non_condors = _parse_schwab_positions(
            condor_legs + [short_call], ACCOUNT, []
        )
        recognized        = len(condors) * 4 + sum(p["legs"] for p in non_condors)
        total_legs_rcvd   = 5   # 4 option legs from condor + 1 single option
        assert recognized == 5
        assert recognized / total_legs_rcvd == pytest.approx(1.0)

    def test_all_unparseable_legs_produce_zero_recognized(self):
        """16 OPTION legs that all fail OCC parsing → 0 recognized → block closures."""
        bad = {
            "instrument": {"assetType": "OPTION", "symbol": "BAD"},
            "longQuantity": 1.0, "shortQuantity": 0.0, "averagePrice": 0.5,
        }
        errors: list = []
        condors, non_condors = _parse_schwab_positions([bad] * 16, ACCOUNT, errors)
        recognized = len(condors) * 4 + sum(p["legs"] for p in non_condors)
        assert recognized == 0
        # reconcile(): recognized_total == 0 → closures_blocked = True

    def test_4_of_16_recognized_ratio_below_threshold(self):
        """1 condor (4 legs) + 12 bad → ratio=0.25 < 0.5 → closures blocked."""
        condor_legs = make_iron_condor_positions("SPY", EXPIRY, 530, 535, 560, 565)
        bad = {
            "instrument": {"assetType": "OPTION", "symbol": "BAD"},
            "longQuantity": 1.0, "shortQuantity": 0.0, "averagePrice": 0.5,
        }
        condors, non_condors = _parse_schwab_positions(
            condor_legs + [bad] * 12, ACCOUNT, []
        )
        recognized        = len(condors) * 4 + sum(p["legs"] for p in non_condors)
        total_legs_rcvd   = 16
        parse_ratio       = recognized / total_legs_rcvd
        assert recognized   == 4
        assert parse_ratio  == pytest.approx(0.25, abs=0.001)
        assert parse_ratio  < 0.5   # → closures blocked in reconcile()

    def test_4_of_8_recognized_ratio_at_boundary(self):
        """1 condor (4 legs) + 4 bad → ratio=0.5 — NOT < 0.5 → closures allowed."""
        condor_legs = make_iron_condor_positions("SPY", EXPIRY, 530, 535, 560, 565)
        bad = {
            "instrument": {"assetType": "OPTION", "symbol": "BAD"},
            "longQuantity": 1.0, "shortQuantity": 0.0, "averagePrice": 0.5,
        }
        condors, non_condors = _parse_schwab_positions(
            condor_legs + [bad] * 4, ACCOUNT, []
        )
        recognized      = len(condors) * 4 + sum(p["legs"] for p in non_condors)
        total_legs_rcvd = 8
        parse_ratio     = recognized / total_legs_rcvd
        assert parse_ratio == pytest.approx(0.5, abs=0.001)
        assert not (parse_ratio < 0.5)  # exactly 0.5 does NOT block closures


# ── 3-strike closure system ───────────────────────────────────────────────────

class TestThreeStrikeSystem:
    """
    Tests for reconcile()'s 3-strike logic.

    A DB position absent from a Schwab snapshot should:
      - Strike 1: increment closure_strikes to 1, do NOT close
      - Strike 2: increment closure_strikes to 2, do NOT close
      - Strike 3: close the position (status='closed')
    """

    def _db_iron_condor(self, closure_strikes: int = 0) -> dict:
        return {
            "id": 101,
            "symbol": "SPY",
            "expiry": EXPIRY_ISO,
            "strategy": "IRON_CONDOR",
            "account_id": ACCOUNT,
            "long_put_strike": 530.0,
            "short_put_strike": 535.0,
            "short_call_strike": 560.0,
            "long_call_strike": 565.0,
            "quantity": 1,
            "fill_credit": 0.75,
            "status": "open",
            "position_key": f"SPY:{EXPIRY_ISO}:530-535:560-565:1",
            "closure_strikes": closure_strikes,
        }

    def test_first_absence_increments_strike_does_not_close(self):
        from data_layer.reconciler import reconcile

        # Schwab returns EMPTY positions for the account
        client = _make_schwab_client([])
        # DB has 1 open condor with 0 existing strikes
        engine, write_calls = _make_mock_engine([self._db_iron_condor(0)])

        summary = reconcile(engine, client)

        assert len(summary["closed"]) == 0, "Should NOT close on first absence"
        # Verify an UPDATE with closure_strikes=1 was issued
        strike_updates = [
            (sql, params) for sql, params in write_calls
            if "closure_strikes" in sql and params.get("strikes") == 1
        ]
        assert len(strike_updates) == 1, "Should record strike 1"

    def test_second_absence_increments_to_2_does_not_close(self):
        from data_layer.reconciler import reconcile

        client = _make_schwab_client([])
        engine, write_calls = _make_mock_engine([self._db_iron_condor(1)])

        summary = reconcile(engine, client)

        assert len(summary["closed"]) == 0
        strike_updates = [
            (sql, params) for sql, params in write_calls
            if "closure_strikes" in sql and params.get("strikes") == 2
        ]
        assert len(strike_updates) == 1, "Should record strike 2"

    def test_third_absence_closes_position(self):
        from data_layer.reconciler import reconcile

        client = _make_schwab_client([])
        engine, write_calls = _make_mock_engine([self._db_iron_condor(2)])

        summary = reconcile(engine, client)

        assert len(summary["closed"]) == 1
        assert summary["closed"][0]["symbol"] == "SPY"
        # Verify the UPDATE sets status='closed'
        close_sqls = [
            sql for sql, _ in write_calls
            if "status" in sql and "closed" in sql
        ]
        assert len(close_sqls) >= 1

    def test_reappearance_resets_strike_count(self):
        """A position that reappears in Schwab should have closure_strikes reset to 0."""
        from data_layer.reconciler import reconcile

        # Build a Schwab position matching the DB condor (same strikes)
        schwab_positions = make_iron_condor_positions(
            "SPY", EXPIRY,
            lp_strike=530, sp_strike=535, sc_strike=560, lc_strike=565,
        )
        client = _make_schwab_client(schwab_positions)
        engine, write_calls = _make_mock_engine([self._db_iron_condor(2)])

        summary = reconcile(engine, client)

        assert len(summary["closed"]) == 0, "Matched position must NOT be closed"
        # An UPDATE resetting closure_strikes=0 must have fired
        reset_sqls = [
            (sql, params) for sql, params in write_calls
            if "closure_strikes" in sql and params.get("id") == 101
            and (
                params.get("strikes") == 0
                or "closure_strikes     = 0" in sql
                or "closure_strikes = 0" in sql
            )
        ]
        assert len(reset_sqls) >= 1, "closure_strikes should be reset to 0 on match"


# ── _match_position — pure function tests ─────────────────────────────────────

class TestMatchPosition:
    def test_equity_matches_by_symbol_and_strategy(self):
        schwab = {"symbol": "AAPL", "strategy": "EQUITY"}
        db = [
            {"id": 1, "symbol": "AAPL", "strategy": "EQUITY",  "expiry": None},
            {"id": 2, "symbol": "MSFT", "strategy": "EQUITY",  "expiry": None},
        ]
        result = _match_position(schwab, db)
        assert result is not None
        assert result["id"] == 1

    def test_equity_no_match_wrong_symbol(self):
        schwab = {"symbol": "TSLA", "strategy": "EQUITY"}
        db = [{"id": 1, "symbol": "AAPL", "strategy": "EQUITY", "expiry": None}]
        assert _match_position(schwab, db) is None

    def test_iron_condor_matches_all_four_strikes(self):
        schwab = {
            "symbol": "SPY", "strategy": "IRON_CONDOR", "expiry": EXPIRY_ISO,
            "long_put_strike": 530.0, "short_put_strike": 535.0,
            "short_call_strike": 560.0, "long_call_strike": 565.0,
        }
        db = [{
            "id": 10, "symbol": "SPY", "strategy": "IRON_CONDOR",
            "expiry": EXPIRY_ISO,
            "long_put_strike": 530.0, "short_put_strike": 535.0,
            "short_call_strike": 560.0, "long_call_strike": 565.0,
        }]
        assert _match_position(schwab, db)["id"] == 10

    def test_iron_condor_no_match_wrong_long_call_strike(self):
        schwab = {
            "symbol": "SPY", "strategy": "IRON_CONDOR", "expiry": EXPIRY_ISO,
            "long_put_strike": 530.0, "short_put_strike": 535.0,
            "short_call_strike": 560.0, "long_call_strike": 565.0,
        }
        db = [{
            "id": 10, "symbol": "SPY", "strategy": "IRON_CONDOR",
            "expiry": EXPIRY_ISO,
            "long_put_strike": 530.0, "short_put_strike": 535.0,
            "short_call_strike": 560.0, "long_call_strike": 570.0,  # differs
        }]
        assert _match_position(schwab, db) is None

    def test_short_option_matches_by_strategy_and_strike(self):
        schwab = {
            "symbol": "SMCI", "strategy": "SHORT_OPTION", "expiry": EXPIRY_ISO,
            "long_put_strike": None, "short_put_strike": None,
            "short_call_strike": 40.0, "long_call_strike": None,
        }
        db = [{
            "id": 20, "symbol": "SMCI", "strategy": "SHORT_OPTION",
            "expiry": EXPIRY_ISO,
            "long_put_strike": None, "short_put_strike": None,
            "short_call_strike": 40.0, "long_call_strike": None,
        }]
        assert _match_position(schwab, db)["id"] == 20

    def test_short_option_no_match_different_strategy(self):
        schwab = {
            "symbol": "SMCI", "strategy": "SHORT_OPTION", "expiry": EXPIRY_ISO,
            "long_put_strike": None, "short_put_strike": None,
            "short_call_strike": 40.0, "long_call_strike": None,
        }
        db = [{
            "id": 20, "symbol": "SMCI", "strategy": "LONG_OPTION",  # wrong strategy
            "expiry": EXPIRY_ISO,
            "long_put_strike": None, "short_put_strike": None,
            "short_call_strike": 40.0, "long_call_strike": None,
        }]
        assert _match_position(schwab, db) is None
