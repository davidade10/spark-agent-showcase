"""
tests/test_candidate_generation.py — Tests for position-aware candidate generation.

Verifies:
  - Open same-symbol same-strategy (IRON_CONDOR) → candidate suppressed
  - No open position → candidate allowed
  - Open same-symbol different-strategy → candidate allowed with context in scan_notes
"""
from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from strategy_engine import candidates as candidates_mod


class _ResultRows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _CandidateFakeConnection:
    """Minimal fake that handles scan_for_candidates queries for one symbol."""

    def __init__(
        self,
        snapshot_id: int = 1,
        positions_rows: List[tuple] = None,
        symbol: str = "SPY",
        underlying_price: float = 100.0,
        iv_rank: float | None = 25.0,
        contracts: List[Dict[str, Any]] = None,
    ):
        self._snapshot_id = snapshot_id
        self._positions_rows = positions_rows or []
        self._symbol = symbol
        self._underlying_price = underlying_price
        self._iv_rank = iv_rank
        self._contracts = contracts or _make_minimal_contracts()

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
            return _ResultRows([SimpleNamespace(price=self._underlying_price)])

        if "SELECT iv_rank FROM underlying_quotes" in s:
            if self._iv_rank is not None:
                return _ResultRows([SimpleNamespace(iv_rank=self._iv_rank)])
            return _ResultRows([])

        if "FROM option_quotes" in s and "dte BETWEEN" in s:
            return _ResultRows(self._contracts)

        return _ResultRows([])


class _MappingRow:
    """Row-like object with _mapping for dict(row._mapping)."""

    def __init__(self, **kwargs):
        self._mapping = kwargs


def _make_minimal_contracts() -> List[Any]:
    """Contract rows that pass delta/width rules for one expiry."""
    expiry = "2026-05-15"
    dte = 38
    # Short put ~0.16 delta, short call ~0.16, wings $5 wide
    return [
        _MappingRow(expiry=expiry, dte=dte, strike=90.0, option_right="P", bid=0.5, ask=0.6, delta=-0.18, gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
        _MappingRow(expiry=expiry, dte=dte, strike=95.0, option_right="P", bid=1.2, ask=1.4, delta=-0.16, gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
        _MappingRow(expiry=expiry, dte=dte, strike=105.0, option_right="C", bid=1.3, ask=1.5, delta=0.16, gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
        _MappingRow(expiry=expiry, dte=dte, strike=110.0, option_right="C", bid=0.6, ask=0.7, delta=0.12, gamma=None, theta=None, vega=None, iv=None, volume=None, open_interest=None),
    ]


class _ConnCtx:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *args):
        return False


class _CandidateFakeEngine:
    def __init__(self, conn: _CandidateFakeConnection):
        self._conn = conn

    def connect(self):
        return _ConnCtx(self._conn)


class TestCandidateGeneration:
    def test_open_same_symbol_same_strategy_suppresses_candidate(self, monkeypatch):
        """Symbol with open IRON_CONDOR must not produce new candidates."""
        conn = _CandidateFakeConnection(
            positions_rows=[("IWM", "IRON_CONDOR")],
            symbol="IWM",
        )
        engine = _CandidateFakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["IWM"])

        assert len(result) == 0
        assert all(c.symbol != "IWM" for c in result)

    def test_no_open_position_candidate_allowed(self, monkeypatch):
        """No open positions → candidate is allowed."""
        conn = _CandidateFakeConnection(
            positions_rows=[],
            symbol="SPY",
        )
        engine = _CandidateFakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["SPY"])

        assert len(result) >= 1
        assert any(c.symbol == "SPY" for c in result)

    def test_open_same_symbol_different_strategy_context_reflected(self, monkeypatch):
        """Symbol with EQUITY (or other non-condor) → candidate allowed, scan_notes reflect it."""
        conn = _CandidateFakeConnection(
            positions_rows=[("SPY", "EQUITY")],
            symbol="SPY",
        )
        engine = _CandidateFakeEngine(conn)
        monkeypatch.setattr(candidates_mod, "create_engine", lambda _: engine)

        result = candidates_mod.scan_for_candidates(symbols=["SPY"])

        assert len(result) >= 1
        spy_candidates = [c for c in result if c.symbol == "SPY"]
        assert len(spy_candidates) >= 1
        notes = " ".join(spy_candidates[0].scan_notes)
        assert "Existing open position" in notes
        assert "EQUITY" in notes
