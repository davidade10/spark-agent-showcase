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
        assert eq["position_key"]      == "AAPL:EQUITY"

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
