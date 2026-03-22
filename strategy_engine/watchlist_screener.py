"""
strategy_engine/watchlist_screener.py

Daily watchlist expansion screener. Evaluates symbols in the extended universe
that are NOT already on the active watchlist and ranks those that pass all five
filters as candidates for watchlist addition (operator review required).

Filters applied in order — first failure stops processing for that symbol:
  1. IV rank > 50
  2. Open interest >= 500 on ATM near-month strike (single latest snapshot)
  3. No earnings event within 30 days
  4. Underlying price > $20
  5. No open position in the positions table

All data sourced from existing DB tables — no external API calls.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import create_engine, text

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from data_layer.collector import WATCHLIST

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Universe ──────────────────────────────────────────────────────────────────

EXTENDED_UNIVERSE: list[str] = [
    "IWM", "DIA", "GLD", "SLV",
    "TLT", "HYG", "LQD", "GDX", "XLE", "XLF",
    "XLK", "XBI", "EEM", "ARKK", "SMH",
    "AAPL", "MSFT", "AMZN", "GOOGL", "META",
    "NVDA", "AMD", "TSLA", "NFLX", "INTC", "QCOM",
    "MU", "BA", "JPM", "GS", "BAC", "WMT", "HD",
    "COST", "SOFI", "PLTR", "HOOD", "COIN",
]

# Symbols already on the watchlist are never screened — they don't need a
# recommendation to join a list they're already on.
def _screen_universe() -> list[str]:
    watchlist_set = set(WATCHLIST)
    return [s for s in EXTENDED_UNIVERSE if s not in watchlist_set]


# ── Per-symbol processor ──────────────────────────────────────────────────────

def _process_symbol(
    conn,
    symbol: str,
    today: date,
    candidates: list[dict],
    filtered_symbols: list[dict],
    skipped_symbols: list[dict],
) -> None:
    """
    Apply all 5 filters for a single symbol. Appends result to the appropriate
    list. Returns without raising — caller handles exceptions at a higher level.
    """

    # ── Filter 1: IV rank > 50 ────────────────────────────────────────────────
    ivr_row = conn.execute(text("""
        SELECT iv_rank FROM underlying_quotes
        WHERE symbol = :symbol
          AND iv_rank IS NOT NULL
        ORDER BY ts DESC
        LIMIT 1
    """), {"symbol": symbol}).fetchone()

    if not ivr_row:
        skipped_symbols.append({"symbol": symbol, "reason": "no iv_rank data available"})
        return

    iv_rank = float(ivr_row.iv_rank)
    if iv_rank <= 50:
        filtered_symbols.append({
            "symbol":        symbol,
            "filter_failed": 1,
            "reason":        f"iv_rank {iv_rank:.1f} below 50 threshold",
        })
        return

    # ── Filter 2a: near-month expiry (14–45 DTE) ──────────────────────────────
    expiry_row = conn.execute(text("""
        SELECT DISTINCT expiry
        FROM option_quotes
        WHERE symbol = :symbol
          AND (expiry - CURRENT_DATE) BETWEEN 14 AND 45
        ORDER BY expiry ASC
        LIMIT 1
    """), {"symbol": symbol}).fetchone()

    if not expiry_row:
        filtered_symbols.append({
            "symbol":        symbol,
            "filter_failed": 2,
            "reason":        "no near-month expiry (14-45 DTE)",
        })
        return

    near_month_expiry: date = expiry_row.expiry
    days_to_expiry = (near_month_expiry - today).days

    # ── Filter 2b: underlying price ───────────────────────────────────────────
    price_row = conn.execute(text("""
        SELECT price FROM underlying_quotes
        WHERE symbol = :symbol
        ORDER BY ts DESC
        LIMIT 1
    """), {"symbol": symbol}).fetchone()

    if not price_row:
        filtered_symbols.append({
            "symbol":        symbol,
            "filter_failed": 2,
            "reason":        "no underlying price data",
        })
        return

    price = float(price_row.price)

    # ── Filter 2c: ATM strike ─────────────────────────────────────────────────
    atm_row = conn.execute(text("""
        SELECT strike FROM option_quotes
        WHERE symbol = :symbol
          AND expiry = :expiry
        ORDER BY ABS(strike - :price) ASC
        LIMIT 1
    """), {"symbol": symbol, "expiry": near_month_expiry, "price": price}).fetchone()

    if not atm_row:
        filtered_symbols.append({
            "symbol":        symbol,
            "filter_failed": 2,
            "reason":        "no option quotes found for near-month expiry",
        })
        return

    atm_strike = float(atm_row.strike)

    # ── Filter 2d: OI at latest snapshot only ─────────────────────────────────
    latest_ts_row = conn.execute(text("""
        SELECT MAX(ts) AS latest_ts FROM option_quotes
        WHERE symbol = :symbol
          AND expiry  = :expiry
          AND strike  = :atm_strike
    """), {"symbol": symbol, "expiry": near_month_expiry,
          "atm_strike": atm_strike}).fetchone()

    if not latest_ts_row or latest_ts_row.latest_ts is None:
        filtered_symbols.append({
            "symbol":        symbol,
            "filter_failed": 2,
            "reason":        "could not determine latest snapshot timestamp",
        })
        return

    oi_row = conn.execute(text("""
        SELECT COALESCE(SUM(open_interest), 0) AS total_oi
        FROM option_quotes
        WHERE symbol = :symbol
          AND expiry  = :expiry
          AND strike  = :atm_strike
          AND ts      = :latest_ts
    """), {"symbol": symbol, "expiry": near_month_expiry,
          "atm_strike": atm_strike,
          "latest_ts": latest_ts_row.latest_ts}).fetchone()

    total_oi = int(oi_row.total_oi) if oi_row else 0

    if total_oi < 500:
        filtered_symbols.append({
            "symbol":        symbol,
            "filter_failed": 2,
            "reason":        f"OI {total_oi} below 500 at ATM strike {atm_strike}",
        })
        return

    # ── Filter 3: no earnings within 30 days ──────────────────────────────────
    earnings_row = conn.execute(text("""
        SELECT DATE(event_ts) AS event_date
        FROM events
        WHERE symbol     = :symbol
          AND event_type = 'earnings'
          AND event_ts   >= CURRENT_DATE
          AND event_ts   < CURRENT_DATE + INTERVAL '31 days'
        ORDER BY event_ts ASC
        LIMIT 1
    """), {"symbol": symbol}).fetchone()

    if earnings_row:
        event_date = earnings_row.event_date
        days_away  = (event_date - today).days
        filtered_symbols.append({
            "symbol":        symbol,
            "filter_failed": 3,
            "reason":        f"earnings in {days_away}d ({event_date})",
        })
        return

    # Lookahead: next earnings within 90 days (for candidate output)
    earnings_90_row = conn.execute(text("""
        SELECT DATE(event_ts) AS event_date
        FROM events
        WHERE symbol     = :symbol
          AND event_type = 'earnings'
          AND event_ts   >= CURRENT_DATE
          AND event_ts   < CURRENT_DATE + INTERVAL '91 days'
        ORDER BY event_ts ASC
        LIMIT 1
    """), {"symbol": symbol}).fetchone()

    next_earnings_date: str | None = None
    days_to_earnings: int | None   = None
    if earnings_90_row:
        next_earnings_date = str(earnings_90_row.event_date)
        days_to_earnings   = (earnings_90_row.event_date - today).days

    # ── Filter 4: price > $20 ─────────────────────────────────────────────────
    if price <= 20.0:
        filtered_symbols.append({
            "symbol":        symbol,
            "filter_failed": 4,
            "reason":        f"price ${price:.2f} below $20 threshold",
        })
        return

    # ── Filter 5: no open position ────────────────────────────────────────────
    pos_row = conn.execute(text("""
        SELECT strategy FROM positions
        WHERE symbol = :symbol
          AND status  = 'open'
        LIMIT 1
    """), {"symbol": symbol}).fetchone()

    if pos_row:
        filtered_symbols.append({
            "symbol":        symbol,
            "filter_failed": 5,
            "reason":        f"open {pos_row.strategy} position exists",
        })
        return

    # ── Passed all filters ────────────────────────────────────────────────────
    candidates.append({
        "rank":               0,  # assigned after sorting
        "symbol":             symbol,
        "iv_rank":            round(iv_rank, 1),
        "open_interest":      total_oi,
        "near_month_expiry":  str(near_month_expiry),
        "days_to_expiry":     days_to_expiry,
        "underlying_price":   round(price, 2),
        "atm_strike":         atm_strike,
        "next_earnings_date": next_earnings_date,
        "days_to_earnings":   days_to_earnings,
        "reason": (
            f"IV rank {iv_rank:.1f} — elevated premium environment; "
            f"OI {total_oi:,} at ${atm_strike} ATM; "
            f"no open position"
        ),
    })


# ── Public entry point ────────────────────────────────────────────────────────

def run_screener() -> dict[str, Any]:
    """
    Screen the extended universe for watchlist candidates.
    Returns a dict with candidates, filtered_symbols, skipped_symbols,
    and run metadata. Never raises — returns an error dict on DB failure.
    """
    run_at = datetime.now(timezone.utc).isoformat()
    today  = date.today()

    screen_universe = _screen_universe()

    candidates:      list[dict] = []
    filtered_symbols: list[dict] = []
    skipped_symbols:  list[dict] = []

    try:
        engine = create_engine(DB_URL, pool_pre_ping=True)

        with engine.connect() as conn:

            # ── Data-availability pre-filter ──────────────────────────────────
            data_available: list[str] = []
            for symbol in screen_universe:
                count = conn.execute(text("""
                    SELECT COUNT(*) FROM option_quotes
                    WHERE symbol = :symbol
                      AND ts >= NOW() - INTERVAL '5 days'
                """), {"symbol": symbol}).scalar() or 0

                if count > 0:
                    data_available.append(symbol)
                else:
                    skipped_symbols.append({
                        "symbol": symbol,
                        "reason": "no option_quotes data in last 5 days",
                    })

            logger.info(
                "Screener: %d symbols in screen universe, %d have recent data",
                len(screen_universe), len(data_available),
            )

            # ── Per-symbol filter pipeline ────────────────────────────────────
            for symbol in data_available:
                try:
                    _process_symbol(
                        conn, symbol, today,
                        candidates, filtered_symbols, skipped_symbols,
                    )
                except Exception as exc:
                    logger.error("Screener: %s — processing error: %s", symbol, exc)
                    skipped_symbols.append({
                        "symbol": symbol,
                        "reason": f"processing error: {exc}",
                    })

    except Exception as exc:
        logger.error("run_screener: DB error — %s", exc)
        return {
            "status":           "error",
            "message":          str(exc),
            "run_at":           run_at,
            "candidates":       [],
            "filtered_symbols": [],
            "skipped_symbols":  [],
        }

    # Sort candidates: iv_rank desc, then OI desc; assign ranks
    candidates.sort(key=lambda c: (-c["iv_rank"], -c["open_interest"]))
    for i, c in enumerate(candidates, 1):
        c["rank"] = i

    result: dict[str, Any] = {
        "run_at":                    run_at,
        "universe_total":            len(EXTENDED_UNIVERSE),
        "active_watchlist_excluded": len(WATCHLIST),
        "screen_universe_size":      len(screen_universe),
        "data_available":            len(data_available),
        "passed":                    len(candidates),
        "filtered_out":              len(filtered_symbols),
        "skipped":                   len(skipped_symbols),
        "candidates":                candidates,
        "filtered_symbols":          filtered_symbols,
        "skipped_symbols":           skipped_symbols,
    }

    logger.info(
        "Screener complete — %d passed, %d filtered, %d skipped",
        len(candidates), len(filtered_symbols), len(skipped_symbols),
    )
    return result
