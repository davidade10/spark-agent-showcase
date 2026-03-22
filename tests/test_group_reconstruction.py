"""
tests/test_group_reconstruction.py — Unit tests for _parse_schwab_positions.

Tests that the position parser correctly groups option legs into recognised
strategies and assigns the right metadata (strategy, legs count, position_key
namespace, fill_credit arithmetic).
"""
import pytest

from tests.conftest import (
    make_option_position,
    make_equity_position,
    make_iron_condor_positions,
)
from data_layer.reconciler import _parse_schwab_positions
from strategy_engine.exit_monitor import compute_position_marks


class _PricingConn:
    def __init__(self, snapshot_id: int, positions_rows, option_quote_rows):
        self._snapshot_id = snapshot_id
        self._positions_rows = positions_rows
        self._option_quote_rows = option_quote_rows

    def execute(self, sql, params=None):
        s = str(sql)
        if "SELECT id FROM snapshot_runs" in s:
            class _R:
                def fetchone(self_inner):
                    return (self._snapshot_id,)
            return _R()
        if "FROM positions" in s and "long_put_strike" in s:
            class _R:
                def fetchall(self_inner):
                    return self._positions_rows
            return _R()
        if "FROM option_quotes" in s and "snapshot_id = :sid" in s:
            class _R:
                def fetchall(self_inner):
                    return self._option_quote_rows
            return _R()
        if "FROM underlying_quotes" in s and "snapshot_id" in s:
            class _R:
                def fetchall(self_inner):
                    return []
            return _R()
        raise AssertionError(f"Unexpected SQL in pricing test: {s}")


class _PricingEngine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        class _Ctx:
            def __init__(self, c): self._c = c
            def __enter__(self): return self._c
            def __exit__(self, exc_type, exc, tb): return False
        return _Ctx(self._conn)


ACCOUNT    = "5760"
EXPIRY     = "260417"
EXPIRY_ISO = "2026-04-17"


# ── Iron condor grouping ───────────────────────────────────────────────────────

class TestIronCondorGrouping:
    def test_4_leg_condor_grouped_correctly(self):
        positions = make_iron_condor_positions(
            "SPY", EXPIRY,
            lp_strike=530, sp_strike=535, sc_strike=560, lc_strike=565,
            qty=2, lp_avg=0.40, sp_avg=0.70, sc_avg=0.80, lc_avg=0.35,
        )
        condors, non_condors = _parse_schwab_positions(positions, ACCOUNT, [])

        assert len(condors)     == 1
        assert len(non_condors) == 0

        c = condors[0]
        assert c["symbol"]            == "SPY"
        assert c["expiry"]            == EXPIRY_ISO
        assert c["strategy"]          == "IRON_CONDOR"
        assert c["quantity"]          == 2
        assert c["long_put_strike"]   == 530.0
        assert c["short_put_strike"]  == 535.0
        assert c["short_call_strike"] == 560.0
        assert c["long_call_strike"]  == 565.0
        assert c["legs"]              == 4
        # Reconciler persists positions.legs JSONB from legs_struct (strategy_engine shape).
        ls = c["legs_struct"]
        assert set(ls.keys()) == {"short_put", "long_put", "short_call", "long_call"}
        for k, strike in (
            ("long_put", 530.0),
            ("short_put", 535.0),
            ("short_call", 560.0),
            ("long_call", 565.0),
        ):
            leg = ls[k]
            assert leg["strike"] == strike
            assert "avg_price" in leg
            assert leg["qty_signed"] != 0

    def test_condor_fill_credit_computed(self):
        # credit = sp_avg + sc_avg - lp_avg - lc_avg = 0.70 + 0.80 - 0.40 - 0.35 = 0.75
        positions = make_iron_condor_positions(
            "SPY", EXPIRY,
            lp_strike=530, sp_strike=535, sc_strike=560, lc_strike=565,
            lp_avg=0.40, sp_avg=0.70, sc_avg=0.80, lc_avg=0.35,
        )
        condors, _ = _parse_schwab_positions(positions, ACCOUNT, [])
        assert condors[0]["fill_credit"] == pytest.approx(0.75, abs=0.001)

    def test_condor_position_key_uses_colon_namespace(self):
        """Iron condor position_key must use the legacy ':' delimiter format."""
        positions = make_iron_condor_positions(
            "META", EXPIRY,
            lp_strike=500, sp_strike=510, sc_strike=560, lc_strike=570,
        )
        condors, _ = _parse_schwab_positions(positions, ACCOUNT, [])
        key = condors[0]["position_key"]
        assert key.startswith("META:")
        assert EXPIRY_ISO in key
        # Ensure the non-condor underscore namespace hasn't leaked in
        after_expiry = key.split(EXPIRY_ISO, 1)[1]
        assert "_" not in after_expiry

    def test_asymmetric_leg_quantities_go_to_errors(self):
        """Mismatched leg quantities must be rejected and recorded in errors."""
        positions = make_iron_condor_positions(
            "SPY", EXPIRY,
            lp_strike=530, sp_strike=535, sc_strike=560, lc_strike=565,
        )
        # Give the short-put leg a different quantity
        positions[1]["shortQuantity"] = 3.0

        errors: list = []
        condors, non_condors = _parse_schwab_positions(positions, ACCOUNT, errors)

        assert len(condors)     == 0
        assert len(errors)      == 1
        assert "asymmetric"     in errors[0].lower()

    def test_split_lots_same_occ_are_merged_and_condor_reconstructed(self):
        """
        Regression: Schwab can return the *same OCC contract* split across multiple
        position rows (tax lots). We must merge by OCC symbol so a valid 4-leg
        iron condor does not fall through to UNKNOWN.
        """
        positions = make_iron_condor_positions(
            "IWM", EXPIRY,
            lp_strike=220, sp_strike=225, sc_strike=269, lc_strike=275,
            qty=2,
            lp_avg=1.10, sp_avg=2.20, sc_avg=2.00, lc_avg=1.20,
        )
        # Split the short put leg into two separate rows (same OCC symbol).
        split_leg = dict(positions[1])
        positions[1]["shortQuantity"] = 1.0
        split_leg["shortQuantity"] = 1.0
        positions.insert(2, split_leg)

        condors, non_condors = _parse_schwab_positions(positions, ACCOUNT, [])
        assert len(condors) == 1
        assert len(non_condors) == 0
        c = condors[0]
        assert c["symbol"] == "IWM"
        assert c["expiry"] == EXPIRY_ISO
        assert c["strategy"] == "IRON_CONDOR"
        assert c["quantity"] == 2
        assert c["long_put_strike"] == 220.0
        assert c["short_put_strike"] == 225.0
        assert c["short_call_strike"] == 269.0
        assert c["long_call_strike"] == 275.0

    def test_iwm_6_leg_shared_put_decomposes_into_two_condors(self):
        """
        Live regression: IWM can appear as two condors sharing the same put spread,
        represented as a 6-leg pool:
          LP 220P qty4, SP 225P qty4,
          SC 269C qty2 + LC 274C qty2,
          SC 270C qty2 + LC 275C qty2
        Must decompose into two independent IRON_CONDOR positions.
        """
        positions = [
            make_option_position("IWM", EXPIRY, "P", 220.0, long_qty=4, avg_price=1.0),
            make_option_position("IWM", EXPIRY, "P", 225.0, short_qty=4, avg_short_price=1.2),
            make_option_position("IWM", EXPIRY, "C", 269.0, short_qty=2, avg_short_price=1.1),
            make_option_position("IWM", EXPIRY, "C", 274.0, long_qty=2, avg_price=1.4),
            make_option_position("IWM", EXPIRY, "C", 270.0, short_qty=2, avg_short_price=1.0),
            make_option_position("IWM", EXPIRY, "C", 275.0, long_qty=2, avg_price=1.3),
        ]
        condors, non_condors = _parse_schwab_positions(positions, "8096", [])
        assert len(non_condors) == 0
        assert len(condors) == 2

        keys = {(c["long_put_strike"], c["short_put_strike"], c["short_call_strike"], c["long_call_strike"], c["quantity"]) for c in condors}
        assert (220.0, 225.0, 269.0, 274.0, 2) in keys
        assert (220.0, 225.0, 270.0, 275.0, 2) in keys

    def test_decomposed_iwm_condors_are_mark_pricable_when_quotes_exist(self):
        """
        Follow-through: once decomposed into IRON_CONDOR rows, exit_monitor's
        compute_position_marks should produce non-null marks when quotes exist.
        """
        snapshot_id = 123
        positions_rows = [
            (1, "IWM", EXPIRY_ISO, "IRON_CONDOR", 220.0, 225.0, 269.0, 274.0),
            (2, "IWM", EXPIRY_ISO, "IRON_CONDOR", 220.0, 225.0, 270.0, 275.0),
        ]
        # option_quotes rows are tuples: (symbol, expiry_text, option_right, strike, bid, ask)
        option_quote_rows = [
            ("IWM", EXPIRY_ISO, "P", 220.0, 1.00, 1.10),
            ("IWM", EXPIRY_ISO, "P", 225.0, 1.20, 1.30),
            ("IWM", EXPIRY_ISO, "C", 269.0, 0.90, 1.00),
            ("IWM", EXPIRY_ISO, "C", 274.0, 0.60, 0.70),
            ("IWM", EXPIRY_ISO, "C", 270.0, 0.85, 0.95),
            ("IWM", EXPIRY_ISO, "C", 275.0, 0.55, 0.65),
        ]
        engine = _PricingEngine(_PricingConn(snapshot_id, positions_rows, option_quote_rows))
        marks = compute_position_marks(engine)
        assert marks[1] is not None
        assert marks[2] is not None


# ── Non-condor option strategies ──────────────────────────────────────────────

class TestNonCondorGrouping:
    def test_single_short_call_is_short_option(self):
        pos = make_option_position("SMCI", EXPIRY, "C", 40.0, short_qty=1, avg_short_price=0.60)
        condors, non_condors = _parse_schwab_positions([pos], ACCOUNT, [])

        assert len(condors)                     == 0
        assert len(non_condors)                 == 1
        assert non_condors[0]["strategy"]       == "SHORT_OPTION"
        assert non_condors[0]["short_call_strike"] == 40.0
        assert non_condors[0]["legs"]           == 1

    def test_single_long_put_is_long_option(self):
        pos = make_option_position("TSLA", EXPIRY, "P", 250.0, long_qty=2, avg_price=3.50)
        _, non_condors = _parse_schwab_positions([pos], ACCOUNT, [])

        assert non_condors[0]["strategy"]      == "LONG_OPTION"
        assert non_condors[0]["long_put_strike"] == 250.0

    def test_equity_position_classified_correctly(self):
        pos = make_equity_position("AAPL", long_qty=100, avg_price=175.50)
        condors, non_condors = _parse_schwab_positions([pos], ACCOUNT, [])

        assert len(condors)            == 0
        assert len(non_condors)        == 1
        eq = non_condors[0]
        assert eq["strategy"]          == "EQUITY"
        assert eq["symbol"]            == "AAPL"
        assert eq["expiry"]            is None
        assert eq["legs"]              == 1
        assert eq["position_key"]      == "AAPL:EQUITY:5760"

    def test_non_condor_position_key_uses_underscore_namespace(self):
        """Non-condor keys must use '_' delimiters to avoid colliding with condor ':' keys."""
        pos = make_option_position("NVDA", EXPIRY, "C", 1050.0, short_qty=1, avg_short_price=2.0)
        _, non_condors = _parse_schwab_positions([pos], ACCOUNT, [])

        key = non_condors[0]["position_key"]
        assert "_" in key
        assert "SHORT_OPTION" in key

    def test_condor_and_short_option_in_same_batch(self):
        """Condor and single-leg position can coexist in one parse call."""
        condor_legs = make_iron_condor_positions("SPY", EXPIRY, 530, 535, 560, 565)
        short_call  = make_option_position("SMCI", EXPIRY, "C", 40.0, short_qty=1)

        condors, non_condors = _parse_schwab_positions(
            condor_legs + [short_call], ACCOUNT, []
        )

        assert len(condors)     == 1
        assert len(non_condors) == 1
        assert condors[0]["symbol"]     == "SPY"
        assert non_condors[0]["symbol"] == "SMCI"

    def test_zero_quantity_option_rows_are_ignored(self):
        """
        Regression: if Schwab returns an OPTION row with both quantities zero,
        it must not form or inflate a position group.
        """
        zero = make_option_position("IWM", EXPIRY, "C", 270.0, long_qty=0, short_qty=0)
        # Also include one real leg so we can assert we only see the real one.
        real = make_option_position("IWM", EXPIRY, "P", 225.0, short_qty=2, avg_short_price=1.0)
        condors, non_condors = _parse_schwab_positions([zero, real], ACCOUNT, [])
        assert len(condors) == 0
        assert len(non_condors) == 1
        assert non_condors[0]["strategy"] == "SHORT_OPTION"
        assert non_condors[0]["symbol"] == "IWM"

    def test_shared_put_pool_with_mismatched_quantities_remains_imbalanced(self):
        """
        Safety: if the pool can't be cleanly partitioned (quantities don't add up),
        do not normalize into multiple condors. It should fall through to UNKNOWN
        and be quarantined as imbalanced.
        """
        positions = [
            make_option_position("IWM", EXPIRY, "P", 220.0, long_qty=4),
            make_option_position("IWM", EXPIRY, "P", 225.0, short_qty=4),
            make_option_position("IWM", EXPIRY, "C", 269.0, short_qty=1),
            make_option_position("IWM", EXPIRY, "C", 274.0, long_qty=1),
            make_option_position("IWM", EXPIRY, "C", 270.0, short_qty=2),
            make_option_position("IWM", EXPIRY, "C", 275.0, long_qty=2),
        ]
        condors, non_condors = _parse_schwab_positions(positions, "8096", [])
        assert len(condors) == 0
        assert len(non_condors) == 1
        assert non_condors[0]["strategy"] == "UNKNOWN"
        assert non_condors[0]["status"] == "imbalanced"
