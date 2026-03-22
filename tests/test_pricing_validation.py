"""
tests/test_pricing_validation.py — Stress tests for snapshot-based pricing on
expanded symbol universe.

Focus:
  - Dynamic symbol inclusion from open positions into collector universe.
  - option_quotes / snapshot_runs wiring for new symbols.
  - exit_monitor mark + unrealized_pnl computation from latest snapshot.
  - Safety when latest snapshot lacks usable quote data.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

import pytest

from data_layer import collector as collector_mod
from strategy_engine import exit_monitor as exit_mod


@dataclass
class FakeDBState:
    positions: List[Dict[str, Any]] = field(default_factory=list)
    snapshot_runs: List[Dict[str, Any]] = field(default_factory=list)
    option_quotes: List[Dict[str, Any]] = field(default_factory=list)
    underlying_quotes: List[Dict[str, Any]] = field(default_factory=list)
    exit_signals: List[Dict[str, Any]] = field(default_factory=list)
    next_snapshot_id: int = 1


class _ResultScalar:
    def __init__(self, value: Any):
        self._value = value

    def scalar(self) -> Any:
        return self._value


class _ResultRows:
    def __init__(self, rows, rowcount: int | None = None):
        self._rows = rows
        self.rowcount = rowcount if rowcount is not None else len(rows)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeSavepoint:
    def commit(self):
        pass

    def rollback(self):
        pass


class FakeConnection:
    def __init__(self, state: FakeDBState):
        self._state = state

    def begin_nested(self):
        return _FakeSavepoint()

    # SQLAlchemy text() is passed in; we operate on its string form.
    def execute(self, sql, params: Dict[str, Any] | None = None):
        params = params or {}
        s = str(sql)

        # Collector: derive dynamic symbols from positions
        if "SELECT DISTINCT symbol" in s and "FROM positions" in s:
            rows = [(p["symbol"],) for p in self._state.positions if p.get("status") == "open"]
            return _ResultRows(rows)

        # Collector: load required contracts from open positions (symbol, expiry, strategy, strikes, legs_json)
        if "FROM positions" in s and "legs_json" in s and "strategy IN" in s and "SELECT" in s and "id" not in s:
            rows = []
            for p in self._state.positions:
                if p.get("status") != "open":
                    continue
                if p.get("strategy", "").upper() not in (
                    "IRON_CONDOR", "SHORT_OPTION", "LONG_OPTION",
                    "VERTICAL_SPREAD", "STRADDLE", "STRANGLE",
                ):
                    continue
                rows.append((
                    p["symbol"],
                    p.get("expiry"),
                    p.get("strategy"),
                    p.get("long_put_strike"),
                    p.get("short_put_strike"),
                    p.get("short_call_strike"),
                    p.get("long_call_strike"),
                    p.get("legs_json"),
                ))
            return _ResultRows(rows)

        # Collector: insert snapshot_runs anchor row
        if "INSERT INTO snapshot_runs" in s and "RETURNING id" in s:
            sid = self._state.next_snapshot_id
            self._state.next_snapshot_id += 1
            self._state.snapshot_runs.append(
                {
                    "id": sid,
                    "status": "running",
                    "meta": params.get("meta"),
                    "ts": params.get("ts") or datetime.now(timezone.utc),
                }
            )
            return _ResultScalar(sid)

        # Collector: update snapshot_runs with final meta/status
        if "UPDATE snapshot_runs" in s and "SET" in s:
            sid = params.get("id")
            for row in self._state.snapshot_runs:
                if row["id"] == sid:
                    row["status"] = params.get("status", row.get("status"))
                    row["meta"] = params.get("meta", row.get("meta"))
            return _ResultRows([])

        # Collector: insert underlying_quotes
        if "INSERT INTO underlying_quotes" in s:
            self._state.underlying_quotes.append(
                {
                    "ts": params.get("ts"),
                    "symbol": params.get("symbol"),
                    "price": params.get("price"),
                    "snapshot_id": params.get("snapshot_id"),
                }
            )
            return _ResultRows([])

        # Collector: insert option_quotes
        if "INSERT INTO option_quotes" in s:
            self._state.option_quotes.append(
                {
                    "ts": params.get("ts"),
                    "snapshot_id": params.get("snapshot_id"),
                    "symbol": params.get("symbol"),
                    "expiry": params.get("expiry"),
                    "dte": params.get("dte"),
                    "strike": params.get("strike"),
                    "option_right": params.get("option_right"),
                    "bid": params.get("bid"),
                    "ask": params.get("ask"),
                }
            )
            return _ResultRows([])

        # Exit monitor: select latest completed snapshot
        if "SELECT id FROM snapshot_runs" in s:
            # Filter to completed snapshots
            eligible = [r for r in self._state.snapshot_runs if r.get("status") in ("ok", "partial")]
            if not eligible:
                return _ResultRows([])
            latest = max(eligible, key=lambda r: r["id"])
            return _ResultRows([(latest["id"],)])

        # Exit monitor: select open positions with strikes and legs_json
        if "FROM positions" in s and "long_put_strike" in s and "short_put_strike" in s:
            rows = []
            for p in self._state.positions:
                if p.get("status") != "open":
                    continue
                rows.append(
                    (
                        p["id"],
                        p["symbol"],
                        p["expiry"],
                        p["strategy"],
                        p.get("long_put_strike"),
                        p.get("short_put_strike"),
                        p.get("short_call_strike"),
                        p.get("long_call_strike"),
                        p.get("legs_json"),
                    )
                )
            return _ResultRows(rows)

        # Exit monitor: select underlying_quotes for snapshot + symbols
        if "FROM underlying_quotes" in s and "snapshot_id" in s:
            sid = params.get("sid")
            symbols = params.get("symbols") or []
            rows = []
            for uq in self._state.underlying_quotes:
                if uq.get("snapshot_id") != sid or uq.get("symbol") not in symbols:
                    continue
                price = uq.get("price")
                if price is None or (isinstance(price, (int, float)) and price <= 0):
                    continue
                rows.append((uq["symbol"], float(price)))
            return _ResultRows(rows)

        # Exit monitor: select option_quotes for snapshot + symbols
        if "FROM option_quotes" in s and "snapshot_id = :sid" in s:
            sid = params.get("sid")
            symbols = params.get("symbols") or []
            rows = []
            for q in self._state.option_quotes:
                if q["snapshot_id"] != sid or q["symbol"] not in symbols:
                    continue
                bid = q.get("bid")
                ask = q.get("ask")
                if bid is None or ask is None:
                    continue
                rows.append(
                    (
                        q["symbol"],
                        str(q["expiry"]),
                        q["option_right"],
                        float(q["strike"]),
                        bid,
                        ask,
                    )
                )
            return _ResultRows(rows)

        # Exit monitor (full scan): select positions with account_id, dte, position_key
        if "FROM positions" in s and "account_id" in s and "position_key" in s:
            rows = []
            for p in self._state.positions:
                if p.get("status") != "open":
                    continue
                rows.append((
                    p["id"],
                    p.get("account_id", "PAPER"),
                    p["symbol"],
                    p.get("expiry"),
                    p.get("strategy", "IRON_CONDOR"),
                    p.get("dte"),
                    p.get("fill_credit"),
                    p.get("quantity", 1),
                    p.get("position_key"),
                ))
            return _ResultRows(rows)

        # Exit monitor (pricing_only): select positions for mark update
        if "FROM positions" in s and "fill_credit" in s and "quantity" in s:
            rows = []
            for p in self._state.positions:
                if p.get("status") != "open":
                    continue
                rows.append((
                    p["id"],
                    p["symbol"],
                    p.get("strategy", "IRON_CONDOR"),
                    p.get("fill_credit"),
                    p.get("quantity", 1),
                ))
            return _ResultRows(rows)

        # Exit monitor: dedupe check for exit_signals
        if "FROM exit_signals" in s and "position_id" in s and "reason" in s:
            pid = params.get("pid")
            reason = params.get("reason")
            for sig in self._state.exit_signals:
                if sig.get("position_id") == pid and sig.get("reason") == reason:
                    if sig.get("status") in ("pending", "acknowledged", "snoozed"):
                        return _ResultRows([(1,)])
            return _ResultRows([])

        # Exit monitor: insert exit_signal
        if "INSERT INTO exit_signals" in s:
            if getattr(self._state, "fail_exit_signal_insert", False):
                raise RuntimeError("Simulated exit_signal insert failure")
            sig_id = len(self._state.exit_signals) + 1
            self._state.exit_signals.append({
                "id": sig_id,
                "position_id": params.get("position_id"),
                "account_id": params.get("account_id"),
                "symbol": params.get("symbol"),
                "reason": params.get("reason"),
                "status": "pending",
            })
            return _ResultRows([(sig_id,)])

        # Exit monitor: update positions with new mark/unrealized_pnl
        if "UPDATE positions" in s and "SET mark" in s:
            pid = params.get("id")
            for p in self._state.positions:
                if p["id"] == pid:
                    p["mark"] = params.get("mark")
                    p["mark_updated_at"] = params.get("now")
                    p["unrealized_pnl"] = params.get("unrealized_pnl")
            return _ResultRows([])

        # Fallback: no-op
        return _ResultRows([])

    def commit(self):
        return None


class _ConnCtx:
    def __init__(self, conn: FakeConnection):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeEngine:
    def __init__(self, state: FakeDBState):
        self._state = state
        self._conn = FakeConnection(state)

    def connect(self):
        return _ConnCtx(self._conn)

    def begin(self):
        return _ConnCtx(self._conn)


class DummyResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class DummyClient:
    class Options:
        class ContractType:
            ALL = "ALL"

    def __init__(self, chains: Dict[str, dict]):
        self._chains = chains

    def get_option_chain(self, symbol: str, contract_type: str, include_underlying_quote: bool = True):
        return DummyResponse(self._chains.get(symbol, {}))


def _make_simple_chain(symbol: str, expiry: str, dte: int, strikes: Dict[float, Dict[str, float]]) -> dict:
    """
    Build a minimal but valid Schwab-style chain for one expiry and a handful of strikes.
    strikes: {strike_price: {"bid": x, "ask": y, "right": "C"/"P"}}
    """
    put_map: Dict[str, Dict[str, list]] = {}
    call_map: Dict[str, Dict[str, list]] = {}
    exp_key = f"{expiry}:{dte}"
    put_strikes: Dict[str, list] = {}
    call_strikes: Dict[str, list] = {}
    for k, v in strikes.items():
        strike_key = f"{k:.1f}"
        contract = {
            "bid": v["bid"],
            "ask": v["ask"],
            "totalVolume": 0,
            "openInterest": 0,
            "delta": None,
            "gamma": None,
            "theta": None,
            "vega": None,
            "volatility": None,
        }
        if v["right"] == "P":
            put_strikes.setdefault(strike_key, []).append(contract)
        else:
            call_strikes.setdefault(strike_key, []).append(contract)
    if put_strikes:
        put_map[exp_key] = put_strikes
    if call_strikes:
        call_map[exp_key] = call_strikes
    return {
        "underlyingPrice": 100.0,
        "putExpDateMap": put_map,
        "callExpDateMap": call_map,
    }


class TestPricingValidation:
    def test_dynamic_symbol_included_in_fresh_snapshot_and_priced(self, monkeypatch):
        """
        A symbol not in the static WATCHLIST appears as an open option position.
        Collector must include it in the snapshot universe and write quotes, and
        exit_monitor must compute mark + unrealized_pnl from that snapshot.
        """
        state = FakeDBState()
        # Use a symbol that is *not* in collector_mod.WATCHLIST.
        dynamic_symbol = "TSLA"
        assert dynamic_symbol not in collector_mod.WATCHLIST

        expiry = "2026-04-17"
        # Single IRON_CONDOR-style position to be priced.
        state.positions.append(
            {
                "id": 1,
                "symbol": dynamic_symbol,
                "expiry": expiry,
                "strategy": "IRON_CONDOR",
                "long_put_strike": 530.0,
                "short_put_strike": 535.0,
                "short_call_strike": 560.0,
                "long_call_strike": 565.0,
                "quantity": 1,
                "fill_credit": 1.50,
                "status": "open",
                "mark": None,
                "unrealized_pnl": None,
            }
        )

        engine = FakeEngine(state)

        # Patch collector to use our fake engine and relax chain validation/DTE window.
        monkeypatch.setattr(collector_mod, "create_engine", lambda _url: engine)
        monkeypatch.setattr(collector_mod, "MIN_STRIKES_OK", 1)
        monkeypatch.setattr(collector_mod, "MIN_STRIKES_PARTIAL", 1)
        monkeypatch.setattr(
            collector_mod,
            "HARD_RULES",
            {"min_dte": 0, "max_dte": 365},
        )

        # Build a simple option chain for TSLA only; other symbols get {} and are skipped.
        strikes = {
            530.0: {"bid": 2.0, "ask": 2.4, "right": "P"},
            535.0: {"bid": 3.0, "ask": 3.4, "right": "P"},
            560.0: {"bid": 3.2, "ask": 3.6, "right": "C"},
            565.0: {"bid": 2.1, "ask": 2.5, "right": "C"},
        }
        chain = _make_simple_chain(dynamic_symbol, expiry, dte=30, strikes=strikes)
        client = DummyClient({dynamic_symbol: chain})

        summary = collector_mod.run_collection_cycle(client)

        # Dynamic symbol must have been included and written successfully.
        assert dynamic_symbol in (summary["symbols_ok"] or summary["symbols_partial"])
        assert any(q["symbol"] == dynamic_symbol for q in state.option_quotes)

        # Compute marks from the latest snapshot and update via pricing_only pass.
        marks = exit_mod.compute_position_marks(engine)
        assert marks, "Expected at least one mark computed"
        mark = marks.get(1)
        assert mark is not None

        # Run pricing-only scan to persist mark + unrealized_pnl.
        exit_mod.run_exit_scan(engine, pricing_only=True)

        pos = state.positions[0]
        assert pos["mark"] is not None
        assert pos["mark"] == pytest.approx(mark)
        expected_mid_sp = (3.0 + 3.4) / 2.0
        expected_mid_sc = (3.2 + 3.6) / 2.0
        expected_mid_lp = (2.0 + 2.4) / 2.0
        expected_mid_lc = (2.1 + 2.5) / 2.0
        expected_mark = expected_mid_sp + expected_mid_sc - expected_mid_lp - expected_mid_lc
        assert mark == pytest.approx(expected_mark, rel=1e-6)

        # Unrealized P&L should follow (fill_credit - mark) * qty * 100.
        expected_pnl = (pos["fill_credit"] - mark) * pos["quantity"] * 100.0
        assert pos["unrealized_pnl"] == pytest.approx(expected_pnl)

    def test_missing_or_unusable_snapshot_data_does_not_corrupt_marks(self, monkeypatch):
        """
        When the latest snapshot lacks usable quote data for a position whose
        previous mark was valid, exit_monitor must *not* overwrite that mark with
        0.0 or treat the position as freshly priced.
        """
        state = FakeDBState()
        symbol = "TSLA"
        expiry = "2026-04-17"

        # Existing open position with a previously computed mark/unrealized_pnl.
        state.positions.append(
            {
                "id": 1,
                "symbol": symbol,
                "expiry": expiry,
                "strategy": "IRON_CONDOR",
                "long_put_strike": 530.0,
                "short_put_strike": 535.0,
                "short_call_strike": 560.0,
                "long_call_strike": 565.0,
                "quantity": 1,
                "fill_credit": 1.50,
                "status": "open",
                "mark": 0.75,
                "unrealized_pnl": (1.50 - 0.75) * 100.0,
            }
        )

        engine = FakeEngine(state)

        # Seed snapshot_runs: an older completed snapshot (id=1) and a new one (id=2)
        now = datetime.now(timezone.utc)
        state.snapshot_runs.append(
            {
                "id": 1,
                "status": "ok",
                "meta": "{}",
                "ts": now - timedelta(minutes=30),
            }
        )
        state.snapshot_runs.append(
            {
                "id": 2,
                "status": "ok",
                "meta": "{}",
                "ts": now,
            }
        )

        # Newest snapshot (id=2) has *no* usable option_quotes rows for this symbol.
        # Leave state.option_quotes empty to simulate missing data.

        # When computing marks, the latest snapshot (id=2) is chosen, but since it
        # has no quotes the mark for this position must be None.
        marks = exit_mod.compute_position_marks(engine)
        assert marks.get(1) is None

        # Run pricing-only scan — it must NOT overwrite the existing good mark.
        old_mark = state.positions[0]["mark"]
        old_pnl = state.positions[0]["unrealized_pnl"]

        exit_mod.run_exit_scan(engine, pricing_only=True)

        pos = state.positions[0]
        assert pos["mark"] == pytest.approx(old_mark)
        assert pos["unrealized_pnl"] == pytest.approx(old_pnl)

    def test_equity_position_pricing(self, monkeypatch):
        """EQUITY positions get mark from underlying_quotes and correct unrealized P/L."""
        state = FakeDBState()
        symbol = "AAPL"
        cost_basis = 175.50
        current_price = 182.25
        quantity = 10

        state.snapshot_runs.append({
            "id": 1,
            "status": "ok",
            "meta": "{}",
            "ts": datetime.now(timezone.utc),
        })
        state.underlying_quotes.append({
            "symbol": symbol,
            "price": current_price,
            "snapshot_id": 1,
        })
        state.positions.append({
            "id": 1,
            "symbol": symbol,
            "expiry": None,
            "strategy": "EQUITY",
            "quantity": quantity,
            "fill_credit": cost_basis,
            "status": "open",
            "mark": None,
            "unrealized_pnl": None,
        })

        engine = FakeEngine(state)
        monkeypatch.setattr(exit_mod, "create_engine", lambda _url: engine)

        marks = exit_mod.compute_position_marks(engine)
        assert marks.get(1) == pytest.approx(current_price, rel=1e-6)

        exit_mod.run_exit_scan(engine, pricing_only=True)

        pos = state.positions[0]
        assert pos["mark"] == pytest.approx(current_price, rel=1e-6)
        expected_pnl = (current_price - cost_basis) * quantity
        assert pos["unrealized_pnl"] == pytest.approx(expected_pnl)

    def test_equity_missing_quote_returns_none(self, monkeypatch):
        """EQUITY with no underlying quote gets mark=None, no fake zeroes."""
        state = FakeDBState()
        state.snapshot_runs.append({
            "id": 1,
            "status": "ok",
            "meta": "{}",
            "ts": datetime.now(timezone.utc),
        })
        state.positions.append({
            "id": 1,
            "symbol": "XYZ",
            "expiry": None,
            "strategy": "EQUITY",
            "quantity": 5,
            "fill_credit": 50.0,
            "status": "open",
            "mark": None,
            "unrealized_pnl": None,
        })
        # No underlying_quotes for XYZ

        engine = FakeEngine(state)
        monkeypatch.setattr(exit_mod, "create_engine", lambda _url: engine)

        marks = exit_mod.compute_position_marks(engine)
        assert marks.get(1) is None

        exit_mod.run_exit_scan(engine, pricing_only=True)
        pos = state.positions[0]
        assert pos["mark"] is None
        assert pos["unrealized_pnl"] is None

    def test_short_option_position_pricing(self, monkeypatch):
        """SHORT_OPTION positions get mark from option_quotes with correct P&L directionality."""
        state = FakeDBState()
        symbol = "NVDA"
        expiry = "2026-05-15"
        strike = 150.0
        fill_credit = 3.50
        quantity = 2
        bid, ask = 2.20, 2.40
        mid = (bid + ask) / 2.0

        state.snapshot_runs.append({
            "id": 1,
            "status": "ok",
            "meta": "{}",
            "ts": datetime.now(timezone.utc),
        })
        state.option_quotes.append({
            "snapshot_id": 1,
            "symbol": symbol,
            "expiry": expiry,
            "strike": strike,
            "option_right": "C",
            "bid": bid,
            "ask": ask,
        })
        state.positions.append({
            "id": 1,
            "symbol": symbol,
            "expiry": expiry,
            "strategy": "SHORT_OPTION",
            "short_call_strike": strike,
            "short_put_strike": None,
            "long_call_strike": None,
            "long_put_strike": None,
            "quantity": quantity,
            "fill_credit": fill_credit,
            "status": "open",
            "mark": None,
            "unrealized_pnl": None,
        })

        engine = FakeEngine(state)
        monkeypatch.setattr(exit_mod, "create_engine", lambda _url: engine)

        marks = exit_mod.compute_position_marks(engine)
        assert marks.get(1) == pytest.approx(mid, rel=1e-6)

        exit_mod.run_exit_scan(engine, pricing_only=True)

        pos = state.positions[0]
        assert pos["mark"] == pytest.approx(mid, rel=1e-6)
        expected_pnl = (fill_credit - mid) * quantity * 100.0
        assert pos["unrealized_pnl"] == pytest.approx(expected_pnl)
        assert expected_pnl > 0, "Lower mark after entry = profit for short option"

    def test_short_option_legs_json_single_leg_pricing(self, monkeypatch):
        """SHORT_OPTION with legs_json (no condor columns) gets mark from option_quotes."""
        import json
        state = FakeDBState()
        symbol = "SMCI"
        expiry = "2026-04-18"
        strike = 950.0
        fill_credit = 12.00
        quantity = 1
        bid, ask = 10.50, 11.50
        mid = (bid + ask) / 2.0
        legs_json = json.dumps([{"symbol": symbol, "expiry": expiry, "option_type": "P", "strike": strike}])

        state.snapshot_runs.append({
            "id": 1,
            "status": "ok",
            "meta": "{}",
            "ts": datetime.now(timezone.utc),
        })
        state.option_quotes.append({
            "snapshot_id": 1,
            "symbol": symbol,
            "expiry": expiry,
            "strike": strike,
            "option_right": "P",
            "bid": bid,
            "ask": ask,
        })
        state.positions.append({
            "id": 1,
            "symbol": symbol,
            "expiry": expiry,
            "strategy": "SHORT_OPTION",
            "short_call_strike": None,
            "short_put_strike": None,
            "long_call_strike": None,
            "long_put_strike": None,
            "legs_json": legs_json,
            "quantity": quantity,
            "fill_credit": fill_credit,
            "status": "open",
            "mark": None,
            "unrealized_pnl": None,
        })

        engine = FakeEngine(state)
        monkeypatch.setattr(exit_mod, "create_engine", lambda _url: engine)

        marks = exit_mod.compute_position_marks(engine)
        assert marks.get(1) == pytest.approx(mid, rel=1e-6)

        exit_mod.run_exit_scan(engine, pricing_only=True)
        pos = state.positions[0]
        assert pos["mark"] == pytest.approx(mid, rel=1e-6)
        expected_pnl = (fill_credit - mid) * quantity * 100.0
        assert pos["unrealized_pnl"] == pytest.approx(expected_pnl)

    def test_short_option_missing_quote_returns_none(self, monkeypatch):
        """SHORT_OPTION with no option quote gets mark=None, no fake zeroes."""
        state = FakeDBState()
        state.snapshot_runs.append({
            "id": 1,
            "status": "ok",
            "meta": "{}",
            "ts": datetime.now(timezone.utc),
        })
        state.positions.append({
            "id": 1,
            "symbol": "RARE",
            "expiry": "2026-06-19",
            "strategy": "SHORT_OPTION",
            "short_call_strike": 100.0,
            "short_put_strike": None,
            "long_call_strike": None,
            "long_put_strike": None,
            "quantity": 1,
            "fill_credit": 2.00,
            "status": "open",
            "mark": None,
            "unrealized_pnl": None,
        })
        # No option_quotes for RARE

        engine = FakeEngine(state)
        monkeypatch.setattr(exit_mod, "create_engine", lambda _url: engine)

        marks = exit_mod.compute_position_marks(engine)
        assert marks.get(1) is None

        exit_mod.run_exit_scan(engine, pricing_only=True)
        pos = state.positions[0]
        assert pos["mark"] is None
        assert pos["unrealized_pnl"] is None

    def test_equity_gets_marks_but_no_condor_signals(self, monkeypatch):
        """EQUITY positions get marks/unrealized_pnl but do NOT generate condor-style exit signals."""
        state = FakeDBState()
        state.snapshot_runs.append({
            "id": 1,
            "status": "ok",
            "meta": "{}",
            "ts": datetime.now(timezone.utc),
        })
        state.underlying_quotes.append({
            "snapshot_id": 1,
            "symbol": "AAPL",
            "price": 175.0,
        })
        state.positions.append({
            "id": 1,
            "account_id": "123456",
            "symbol": "AAPL",
            "expiry": None,
            "strategy": "EQUITY",
            "dte": None,
            "fill_credit": 150.0,
            "quantity": 10,
            "position_key": "AAPL_EQUITY_123456_123456",
            "status": "open",
            "mark": None,
            "unrealized_pnl": None,
        })

        engine = FakeEngine(state)
        monkeypatch.setattr(exit_mod, "create_engine", lambda _url: engine)

        exit_mod.run_exit_scan(engine, pricing_only=False)

        pos = state.positions[0]
        assert pos["mark"] == pytest.approx(175.0)
        expected_pnl = (175.0 - 150.0) * 10
        assert pos["unrealized_pnl"] == pytest.approx(expected_pnl)
        assert len(state.exit_signals) == 0, "EQUITY must not generate condor-style signals"

    def test_short_option_gets_marks_but_no_condor_signals(self, monkeypatch):
        """SHORT_OPTION positions get marks but do NOT generate condor-style exit signals."""
        import json
        state = FakeDBState()
        symbol = "SMCI"
        expiry = "2026-04-18"
        strike = 950.0
        fill_credit = 12.00
        quantity = 1
        bid, ask = 10.50, 11.50
        mid = (bid + ask) / 2.0
        legs_json = json.dumps([{"symbol": symbol, "expiry": expiry, "option_type": "P", "strike": strike}])

        state.snapshot_runs.append({"id": 1, "status": "ok", "meta": "{}", "ts": datetime.now(timezone.utc)})
        state.option_quotes.append({
            "snapshot_id": 1, "symbol": symbol, "expiry": expiry, "strike": strike,
            "option_right": "P", "bid": bid, "ask": ask,
        })
        state.positions.append({
            "id": 1,
            "account_id": "789",
            "symbol": symbol,
            "expiry": expiry,
            "strategy": "SHORT_OPTION",
            "dte": 30,
            "short_call_strike": None,
            "short_put_strike": None,
            "long_call_strike": None,
            "long_put_strike": None,
            "legs_json": legs_json,
            "quantity": quantity,
            "fill_credit": fill_credit,
            "position_key": "SMCI_SHORT_OPTION_789_789",
            "status": "open",
            "mark": None,
            "unrealized_pnl": None,
        })

        engine = FakeEngine(state)
        monkeypatch.setattr(exit_mod, "create_engine", lambda _url: engine)

        exit_mod.run_exit_scan(engine, pricing_only=False)

        pos = state.positions[0]
        assert pos["mark"] == pytest.approx(mid)
        assert len(state.exit_signals) == 0, "SHORT_OPTION must not generate condor-style signals"

    def test_exit_signals_insert_includes_account_id(self, monkeypatch):
        """When a condor triggers a signal, the insert includes account_id."""
        state = FakeDBState()
        symbol = "NVDA"
        expiry = "2026-05-15"
        state.snapshot_runs.append({"id": 1, "status": "ok", "meta": "{}", "ts": datetime.now(timezone.utc)})
        state.option_quotes.extend([
            {"snapshot_id": 1, "symbol": symbol, "expiry": expiry, "strike": 530.0, "option_right": "P", "bid": 0.7, "ask": 0.9},
            {"snapshot_id": 1, "symbol": symbol, "expiry": expiry, "strike": 535.0, "option_right": "P", "bid": 0.9, "ask": 1.1},
            {"snapshot_id": 1, "symbol": symbol, "expiry": expiry, "strike": 560.0, "option_right": "C", "bid": 0.9, "ask": 1.1},
            {"snapshot_id": 1, "symbol": symbol, "expiry": expiry, "strike": 565.0, "option_right": "C", "bid": 0.4, "ask": 0.6},
        ])
        state.positions.append({
            "id": 1,
            "account_id": "LIVE-ACCT-001",
            "symbol": symbol,
            "expiry": expiry,
            "strategy": "IRON_CONDOR",
            "dte": 45,
            "long_put_strike": 530.0,
            "short_put_strike": 535.0,
            "short_call_strike": 560.0,
            "long_call_strike": 565.0,
            "quantity": 1,
            "fill_credit": 1.50,
            "position_key": "NVDA_IC_001_001",
            "status": "open",
        })

        engine = FakeEngine(state)
        monkeypatch.setattr(exit_mod, "create_engine", lambda _url: engine)

        exit_mod.run_exit_scan(engine, pricing_only=False)

        assert len(state.exit_signals) >= 1
        sig = state.exit_signals[0]
        assert sig["account_id"] == "LIVE-ACCT-001"

    def test_failed_signal_insert_preserves_marks(self, monkeypatch):
        """A failed exit_signals insert does not prevent positions.mark from persisting."""
        state = FakeDBState()
        state.fail_exit_signal_insert = True  # type: ignore[attr-defined]
        state.snapshot_runs.append({"id": 1, "status": "ok", "meta": "{}", "ts": datetime.now(timezone.utc)})
        state.option_quotes.extend([
            {"snapshot_id": 1, "symbol": "NVDA", "expiry": "2026-05-15", "strike": 530.0, "option_right": "P", "bid": 0.7, "ask": 0.9},
            {"snapshot_id": 1, "symbol": "NVDA", "expiry": "2026-05-15", "strike": 535.0, "option_right": "P", "bid": 0.9, "ask": 1.1},
            {"snapshot_id": 1, "symbol": "NVDA", "expiry": "2026-05-15", "strike": 560.0, "option_right": "C", "bid": 0.9, "ask": 1.1},
            {"snapshot_id": 1, "symbol": "NVDA", "expiry": "2026-05-15", "strike": 565.0, "option_right": "C", "bid": 0.4, "ask": 0.6},
        ])
        state.positions.append({
            "id": 1,
            "account_id": "PAPER",
            "symbol": "NVDA",
            "expiry": "2026-05-15",
            "strategy": "IRON_CONDOR",
            "dte": 45,
            "long_put_strike": 530.0,
            "short_put_strike": 535.0,
            "short_call_strike": 560.0,
            "long_call_strike": 565.0,
            "quantity": 1,
            "fill_credit": 1.50,
            "position_key": "NVDA_IC_PAPER",
            "status": "open",
        })

        engine = FakeEngine(state)
        monkeypatch.setattr(exit_mod, "create_engine", lambda _url: engine)

        exit_mod.run_exit_scan(engine, pricing_only=False)

        pos = state.positions[0]
        assert pos["mark"] is not None, "Mark must persist even when signal insert fails"
        assert pos["mark"] == pytest.approx(0.7, rel=1e-2)

    def test_condor_still_generates_signals(self, monkeypatch):
        """IRON_CONDOR positions with triggered conditions still generate exit signals."""
        state = FakeDBState()
        symbol = "NVDA"
        expiry = "2026-05-15"
        state.snapshot_runs.append({"id": 1, "status": "ok", "meta": "{}", "ts": datetime.now(timezone.utc)})
        state.option_quotes.extend([
            {"snapshot_id": 1, "symbol": symbol, "expiry": expiry, "strike": 530.0, "option_right": "P", "bid": 0.7, "ask": 0.9},
            {"snapshot_id": 1, "symbol": symbol, "expiry": expiry, "strike": 535.0, "option_right": "P", "bid": 0.9, "ask": 1.1},
            {"snapshot_id": 1, "symbol": symbol, "expiry": expiry, "strike": 560.0, "option_right": "C", "bid": 0.9, "ask": 1.1},
            {"snapshot_id": 1, "symbol": symbol, "expiry": expiry, "strike": 565.0, "option_right": "C", "bid": 0.4, "ask": 0.6},
        ])
        state.positions.append({
            "id": 1,
            "account_id": "PAPER",
            "symbol": symbol,
            "expiry": expiry,
            "strategy": "IRON_CONDOR",
            "dte": 45,
            "long_put_strike": 530.0,
            "short_put_strike": 535.0,
            "short_call_strike": 560.0,
            "long_call_strike": 565.0,
            "quantity": 1,
            "fill_credit": 1.50,
            "position_key": "NVDA_IC_PAPER",
            "status": "open",
        })

        engine = FakeEngine(state)
        monkeypatch.setattr(exit_mod, "create_engine", lambda _url: engine)

        signals = exit_mod.run_exit_scan(engine, pricing_only=False)

        assert len(signals) >= 1
        assert any(s["reason"] == "PROFIT_TARGET" for s in signals)

    def test_collector_force_includes_far_dated_short_option_contract(self, monkeypatch):
        """
        Open short option outside normal DTE window (e.g. SMCI 2026-05-15 C 45)
        must have its quote written so exit_monitor can price it.
        """
        import json
        state = FakeDBState()
        symbol = "SMCI"
        expiry = "2026-05-15"
        strike = 45.0
        dte_far = 70  # outside max_dte (50)
        legs_json = json.dumps([{"symbol": symbol, "expiry": expiry, "option_type": "C", "strike": strike}])

        state.positions.append({
            "id": 1,
            "symbol": symbol,
            "expiry": expiry,
            "strategy": "SHORT_OPTION",
            "short_call_strike": None,
            "short_put_strike": None,
            "long_call_strike": None,
            "long_put_strike": None,
            "legs_json": legs_json,
            "quantity": 1,
            "fill_credit": 2.50,
            "status": "open",
        })

        chain = _make_simple_chain(symbol, expiry, dte_far, {
            45.0: {"bid": 2.0, "ask": 2.4, "right": "C"},
        })
        client = DummyClient({symbol: chain})

        monkeypatch.setattr(collector_mod, "create_engine", lambda _url: FakeEngine(state))
        monkeypatch.setattr(collector_mod, "MIN_STRIKES_OK", 1)
        monkeypatch.setattr(collector_mod, "MIN_STRIKES_PARTIAL", 1)

        summary = collector_mod.run_collection_cycle(client)

        assert symbol in (summary["symbols_ok"] or summary["symbols_partial"])
        matching = [q for q in state.option_quotes if q["symbol"] == symbol and q["expiry"] == expiry and q["strike"] == strike and q["option_right"] == "C"]
        assert len(matching) >= 1, f"Collector must write SMCI {expiry} C {strike} (far-dated outside DTE window)"

        # Exit monitor can now price it
        marks = exit_mod.compute_position_marks(FakeEngine(state))
        assert marks.get(1) is not None
        assert marks.get(1) == pytest.approx(2.2, rel=1e-2)

    def test_collector_force_includes_near_expiry_short_option_contract(self, monkeypatch):
        """
        Open short option near expiry (e.g. STUB 2026-03-20 C 22.5, DTE 7)
        must have its quote written so exit_monitor can price it.
        """
        import json
        state = FakeDBState()
        symbol = "STUB"
        expiry = "2026-03-20"
        strike = 22.5
        dte_near = 7  # outside min_dte (21)

        legs_json = json.dumps([{"symbol": symbol, "expiry": expiry, "option_type": "C", "strike": strike}])
        state.positions.append({
            "id": 1,
            "symbol": symbol,
            "expiry": expiry,
            "strategy": "SHORT_OPTION",
            "short_call_strike": None,
            "short_put_strike": None,
            "long_call_strike": None,
            "long_put_strike": None,
            "legs_json": legs_json,
            "quantity": 1,
            "fill_credit": 1.50,
            "status": "open",
        })

        chain = _make_simple_chain(symbol, expiry, dte_near, {
            22.5: {"bid": 0.30, "ask": 0.40, "right": "C"},
        })
        client = DummyClient({symbol: chain})

        monkeypatch.setattr(collector_mod, "create_engine", lambda _url: FakeEngine(state))
        monkeypatch.setattr(collector_mod, "MIN_STRIKES_OK", 1)
        monkeypatch.setattr(collector_mod, "MIN_STRIKES_PARTIAL", 1)

        summary = collector_mod.run_collection_cycle(client)

        assert symbol in (summary["symbols_ok"] or summary["symbols_partial"])
        matching = [q for q in state.option_quotes if q["symbol"] == symbol and q["expiry"] == expiry and q["strike"] == strike and q["option_right"] == "C"]
        assert len(matching) >= 1, f"Collector must write STUB {expiry} C {strike} (near-expiry outside DTE window)"

        marks = exit_mod.compute_position_marks(FakeEngine(state))
        assert marks.get(1) is not None
        assert marks.get(1) == pytest.approx(0.35, rel=1e-2)

