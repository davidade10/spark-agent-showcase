"""
tests/test_strangle_candidates.py

Tests for StrangleCandidate generation in strategy_engine/candidates.py.

Verifies:
  - Delta selection: strikes within [0.10, 0.22] band chosen correctly
  - IV rank below strangle_min_iv_rank threshold → no candidates generated
  - Net credit below strangle_min_credit → no candidates generated
  - Delta fully outside band (abs < 0.10) → no candidate
  - Symbol with open STRANGLE → suppressed
  - net_credit = sum of short put mid + short call mid
  - StrangleCandidate fields populated correctly
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from strategy_engine import candidates as candidates_mod
from strategy_engine.candidates import StrangleCandidate


# ── Shared fake DB infrastructure ─────────────────────────────────────────────

class _ResultRows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _MappingRow:
    """Row-like object with _mapping for dict(row._mapping)."""
    def __init__(self, **kwargs):
        self._mapping = kwargs


class _StrangleFakeConnection:
    """
    Fake DB connection that drives generate_strangle_candidates() and
    scan_for_candidates() for a single symbol.
    """

    def __init__(
        self,
        snapshot_id: int = 1,
        positions_rows: List[tuple] = None,
        symbol: str = "IWM",
        underlying_price: float = 200.0,
        iv_rank: Optional[float] = 65.0,
        contracts: List[Any] = None,
    ):
        self._snapshot_id   = snapshot_id
        self._positions_rows = positions_rows or []
        self._symbol         = symbol
        self._underlying     = underlying_price
        self._iv_rank        = iv_rank
        self._contracts      = contracts if contracts is not None else _default_contracts()

    def execute(self, sql, params=None):
        params = params or {}
        s = str(sql)

        if "SELECT id FROM snapshot_runs" in s:
            return _ResultRows([SimpleNamespace(id=self._snapshot_id)])

        if "SELECT symbol" in s and "FROM positions" in s and "status" in s:
            rows = [
                SimpleNamespace(symbol=sym, strategy=strat)
                for sym, strat in self._positions_rows
            ]
            return _ResultRows(rows)

        if "SELECT DISTINCT symbol FROM option_quotes" in s:
            return _ResultRows([SimpleNamespace(symbol=self._symbol)])

        if "SELECT price FROM underlying_quotes" in s:
            return _ResultRows([SimpleNamespace(price=self._underlying)])

        if "SELECT iv_rank FROM underlying_quotes" in s:
            if self._iv_rank is not None:
                return _ResultRows([SimpleNamespace(iv_rank=self._iv_rank)])
            return _ResultRows([])

        if "FROM option_quotes" in s and "dte BETWEEN" in s:
            return _ResultRows(self._contracts)

        return _ResultRows([])


class _ConnCtx:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *args):
        return False


class _FakeEngine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return _ConnCtx(self._conn)


# ── Contract factories ─────────────────────────────────────────────────────────

def _default_contracts(expiry="2026-05-15", dte=38):
    """
    Contracts that produce a valid strangle:
      Short put  at strike 185, delta=-0.18 (within [0.10, 0.22])
      Short call at strike 215, delta=+0.18 (within [0.10, 0.22])
      Net credit = (1.00+1.20)/2 + (1.10+1.30)/2 = 1.10 + 1.20 = 2.30
    Also includes an iron condor wing row that should be ignored (no effect on strangle).
    """
    return [
        _MappingRow(expiry=expiry, dte=dte, strike=185.0, option_right="P",
                    bid=1.00, ask=1.20, delta=-0.18,
                    gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
        _MappingRow(expiry=expiry, dte=dte, strike=215.0, option_right="C",
                    bid=1.10, ask=1.30, delta=0.18,
                    gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
        # Additional strikes outside band — should not be chosen as short strikes
        _MappingRow(expiry=expiry, dte=dte, strike=175.0, option_right="P",
                    bid=0.30, ask=0.40, delta=-0.08,
                    gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
        _MappingRow(expiry=expiry, dte=dte, strike=225.0, option_right="C",
                    bid=0.25, ask=0.35, delta=0.07,
                    gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
    ]


def _boundary_contracts(expiry="2026-05-15", dte=38):
    """
    Contracts where the short strikes are exactly at the 0.22 delta boundary.
    These should be selected (HARD_RULES["max_short_delta"] = 0.22 → included).
    """
    return [
        _MappingRow(expiry=expiry, dte=dte, strike=188.0, option_right="P",
                    bid=1.20, ask=1.40, delta=-0.22,
                    gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
        _MappingRow(expiry=expiry, dte=dte, strike=212.0, option_right="C",
                    bid=1.10, ask=1.30, delta=0.22,
                    gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
    ]


def _out_of_band_contracts(expiry="2026-05-15", dte=38):
    """
    Contracts where delta is fully outside the [0.10, 0.22] band.
    No valid short strikes → no candidate.
    """
    return [
        _MappingRow(expiry=expiry, dte=dte, strike=170.0, option_right="P",
                    bid=0.20, ask=0.30, delta=-0.07,
                    gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
        _MappingRow(expiry=expiry, dte=dte, strike=230.0, option_right="C",
                    bid=0.18, ask=0.28, delta=0.06,
                    gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
    ]


def _low_credit_contracts(expiry="2026-05-15", dte=38):
    """
    Contracts with valid delta but credit sum below strangle_min_credit (1.50).
    """
    return [
        _MappingRow(expiry=expiry, dte=dte, strike=185.0, option_right="P",
                    bid=0.30, ask=0.40, delta=-0.16,
                    gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
        _MappingRow(expiry=expiry, dte=dte, strike=215.0, option_right="C",
                    bid=0.25, ask=0.35, delta=0.16,
                    gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
    ]


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestStrangleDeltaSelection:

    def test_valid_strikes_in_band_produce_candidate(self, monkeypatch):
        """Strikes within [0.10, 0.22] delta band should produce a StrangleCandidate."""
        conn   = _StrangleFakeConnection(iv_rank=70.0)
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) >= 1, "Expected at least one strangle candidate"
        s = strangles[0]
        assert s.symbol == "IWM"
        assert s.strategy == "STRANGLE"

    def test_boundary_delta_0_22_is_selected(self, monkeypatch):
        """
        A strike at exactly delta=±0.22 (max_short_delta boundary) must be
        accepted — the filter is inclusive on the upper bound.
        """
        conn   = _StrangleFakeConnection(iv_rank=70.0, contracts=_boundary_contracts())
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) >= 1
        s = strangles[0]
        assert abs(s.short_put_delta)  <= 0.22
        assert abs(s.short_call_delta) <= 0.22

    def test_delta_outside_band_produces_no_candidate(self, monkeypatch):
        """
        All strikes have |delta| < 0.10 — outside the acceptance band.
        No strangle candidate should be produced.
        """
        conn   = _StrangleFakeConnection(iv_rank=70.0, contracts=_out_of_band_contracts())
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) == 0

    def test_short_put_is_closest_to_target_delta(self, monkeypatch):
        """Best delta match (closest to 0.16) is selected, not just any in-band strike."""
        contracts = [
            _MappingRow(expiry="2026-05-15", dte=38, strike=182.0, option_right="P",
                        bid=1.5, ask=1.7, delta=-0.20,
                        gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
            _MappingRow(expiry="2026-05-15", dte=38, strike=185.0, option_right="P",
                        bid=1.2, ask=1.4, delta=-0.16,  # closest to target
                        gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
            _MappingRow(expiry="2026-05-15", dte=38, strike=215.0, option_right="C",
                        bid=1.1, ask=1.3, delta=0.16,
                        gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
        ]
        conn   = _StrangleFakeConnection(iv_rank=70.0, contracts=contracts)
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) >= 1
        # Strike 185 (delta -0.16) is closer to 0.16 than 182 (delta -0.20)
        assert strangles[0].short_put_strike == 185.0


class TestStrangleIvRankGate:

    def test_iv_rank_below_threshold_produces_no_candidates(self, monkeypatch):
        """
        IV rank 35 with strangle_min_iv_rank fallback of 50 → no candidates.
        Low IV doesn't justify unlimited-risk strangle exposure.
        """
        conn   = _StrangleFakeConnection(iv_rank=35.0)
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) == 0, (
            f"Expected 0 strangles with IV rank 35 < threshold 50, got {len(strangles)}"
        )

    def test_iv_rank_at_threshold_produces_candidates(self, monkeypatch):
        """IV rank exactly at threshold (50) should pass — inclusive check."""
        conn   = _StrangleFakeConnection(iv_rank=50.0)
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) >= 1

    def test_iv_rank_none_passes_iv_gate(self, monkeypatch):
        """
        None iv_rank (insufficient history) should NOT trigger the IV rank gate
        — we can't block based on data we don't have.
        """
        conn   = _StrangleFakeConnection(iv_rank=None)
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) >= 1


class TestStrangleCreditGate:

    def test_net_credit_below_minimum_produces_no_candidate(self, monkeypatch):
        """
        Contracts with bid/ask such that net_credit < 1.50 (fallback minimum)
        should produce no strangle candidate.
        """
        conn   = _StrangleFakeConnection(iv_rank=70.0, contracts=_low_credit_contracts())
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) == 0

    def test_net_credit_is_sum_of_both_mid_prices(self, monkeypatch):
        """net_credit must equal short_put_credit + short_call_credit (both at mid)."""
        conn   = _StrangleFakeConnection(iv_rank=70.0)
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) >= 1
        s = strangles[0]
        expected = round(s.short_put_credit + s.short_call_credit, 4)
        assert abs(s.net_credit - expected) < 1e-6, (
            f"net_credit {s.net_credit} != put_credit {s.short_put_credit} "
            f"+ call_credit {s.short_call_credit}"
        )


class TestStrangleSuppression:

    def test_open_strangle_suppresses_new_candidate(self, monkeypatch):
        """Symbol with open STRANGLE must not produce a new strangle candidate."""
        conn   = _StrangleFakeConnection(
            positions_rows=[("IWM", "STRANGLE")],
            iv_rank=70.0,
        )
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) == 0

    def test_open_condor_does_not_suppress_strangle(self, monkeypatch):
        """
        An open IRON_CONDOR blocks a new condor but must NOT block a strangle.
        Both strategies are independent.
        """
        conn   = _StrangleFakeConnection(
            positions_rows=[("IWM", "IRON_CONDOR")],
            iv_rank=70.0,
        )
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) >= 1


class TestStrangleCandidateFields:

    def test_strategy_field_is_strangle(self, monkeypatch):
        conn   = _StrangleFakeConnection(iv_rank=70.0)
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert all(s.strategy == "STRANGLE" for s in strangles)

    def test_no_wing_strikes_on_strangle(self, monkeypatch):
        """StrangleCandidate must not have long_put_strike or long_call_strike attrs."""
        conn   = _StrangleFakeConnection(iv_rank=70.0)
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) >= 1
        s = strangles[0]
        assert not hasattr(s, "long_put_strike")
        assert not hasattr(s, "long_call_strike")
        assert not hasattr(s, "spread_width")
        assert not hasattr(s, "max_loss")

    def test_qty_defaults_to_1(self, monkeypatch):
        conn   = _StrangleFakeConnection(iv_rank=70.0)
        engine = _FakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])
        strangles = [c for c in result if isinstance(c, StrangleCandidate)]

        assert len(strangles) >= 1
        assert strangles[0].qty == 1
