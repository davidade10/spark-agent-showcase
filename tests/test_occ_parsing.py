"""
tests/test_occ_parsing.py — Unit tests for _parse_occ_symbol.

Tests the OCC standard character-position parser:
  [0:6]  root (strip trailing spaces)
  [6:12] YYMMDD expiry
  [12]   C or P
  [13:21] strike * 1000 (8 digits, zero-padded)
"""
import pytest

from data_layer.reconciler import _parse_occ_symbol


class TestParseOccSymbol:
    def test_standard_meta_call(self):
        result = _parse_occ_symbol("META  260417C00735000")
        assert result["root"] == "META"
        assert result["expiry"] == "2026-04-17"
        assert result["option_type"] == "C"
        assert result["strike"] == 735.0

    def test_padded_root_be_put(self):
        result = _parse_occ_symbol("BE    260417P00045000")
        assert result["root"] == "BE"
        assert result["expiry"] == "2026-04-17"
        assert result["option_type"] == "P"
        assert result["strike"] == 45.0

    def test_fractional_strike(self):
        result = _parse_occ_symbol("SPY   260417P00540500")
        assert result["strike"] == pytest.approx(540.5, abs=0.001)

    def test_single_char_root(self):
        result = _parse_occ_symbol("X     260417C00025000")
        assert result["root"] == "X"
        assert result["strike"] == 25.0

    def test_nvda_call_real_format(self):
        result = _parse_occ_symbol("NVDA  260515C01050000")
        assert result["root"] == "NVDA"
        assert result["expiry"] == "2026-05-15"
        assert result["option_type"] == "C"
        assert result["strike"] == pytest.approx(1050.0, abs=0.001)

    def test_smci_covered_call(self):
        result = _parse_occ_symbol("SMCI  260515C00040000")
        assert result["root"] == "SMCI"
        assert result["expiry"] == "2026-05-15"
        assert result["option_type"] == "C"
        assert result["strike"] == 40.0

    # ── Error cases ───────────────────────────────────────────────────────────

    def test_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            _parse_occ_symbol("META260417C")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            _parse_occ_symbol("")

    def test_invalid_option_type_raises(self):
        # Replace the option type byte with 'X'
        with pytest.raises(ValueError, match="Invalid OCC option type"):
            _parse_occ_symbol("META  260417X00735000")

    def test_none_raises(self):
        with pytest.raises((ValueError, AttributeError)):
            _parse_occ_symbol(None)  # type: ignore[arg-type]
