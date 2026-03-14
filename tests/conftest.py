"""
tests/conftest.py — Shared fixtures for Spark Agent test suite.

Provides mock Schwab position builders so individual test files can
construct API-shaped dicts without repeating boilerplate.
"""
import pytest


# ── Schwab mock position builders ─────────────────────────────────────────────

def make_option_position(
    symbol: str,
    expiry_yymmdd: str,
    option_type: str,
    strike: float,
    long_qty: int = 0,
    short_qty: int = 0,
    avg_price: float = 0.0,
    avg_short_price: float = 0.0,
) -> dict:
    """Build a mock Schwab option position dict matching the real API shape."""
    occ_symbol = f"{symbol:<6}{expiry_yymmdd}{option_type}{int(strike * 1000):08d}"
    pos = {
        "instrument": {"assetType": "OPTION", "symbol": occ_symbol},
        "longQuantity":  float(long_qty),
        "shortQuantity": float(short_qty),
        "averagePrice":  avg_price,
    }
    if avg_short_price:
        pos["averageShortPrice"] = avg_short_price
    if avg_price and long_qty:
        pos["averageLongPrice"] = avg_price
    return pos


def make_equity_position(
    symbol: str,
    long_qty: int = 0,
    short_qty: int = 0,
    avg_price: float = 0.0,
) -> dict:
    """Build a mock Schwab equity position dict."""
    return {
        "instrument": {"assetType": "EQUITY", "symbol": symbol},
        "longQuantity":  float(long_qty),
        "shortQuantity": float(short_qty),
        "averagePrice":  avg_price,
        "averageLongPrice": avg_price,
    }


def make_iron_condor_positions(
    symbol: str,
    expiry_yymmdd: str,
    lp_strike: float,
    sp_strike: float,
    sc_strike: float,
    lc_strike: float,
    qty: int = 1,
    lp_avg: float = 0.50,
    sp_avg: float = 0.80,
    sc_avg: float = 0.70,
    lc_avg: float = 0.40,
) -> list:
    """Build 4 mock positions forming a complete iron condor."""
    return [
        make_option_position(symbol, expiry_yymmdd, "P", lp_strike, long_qty=qty,  avg_price=lp_avg),
        make_option_position(symbol, expiry_yymmdd, "P", sp_strike, short_qty=qty, avg_short_price=sp_avg),
        make_option_position(symbol, expiry_yymmdd, "C", sc_strike, short_qty=qty, avg_short_price=sc_avg),
        make_option_position(symbol, expiry_yymmdd, "C", lc_strike, long_qty=qty,  avg_price=lc_avg),
    ]
