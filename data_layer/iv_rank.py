"""
data_layer/iv_rank.py

IV Rank computation (V3 Architecture Compliant).
Calculates the current IV by averaging the 'iv' column across all option legs
in the option_quotes table, then computes the 252-snapshot rank.
"""

import logging
from sqlalchemy import text, create_engine
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
WATCHLIST = ["SPY", "QQQ", "IWM", "NVDA", "AAPL"]

_RANK_QUERY = text("""
    WITH snapshot_iv AS (
        SELECT 
            ts, 
            AVG(iv) as avg_iv 
        FROM option_quotes 
        WHERE symbol = :symbol 
          AND iv IS NOT NULL
        GROUP BY ts
    ),
    windowed AS (
        SELECT
            ts,
            avg_iv,
            MIN(avg_iv) OVER (
                ORDER BY ts
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) AS iv_252d_low,
            MAX(avg_iv) OVER (
                ORDER BY ts
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) AS iv_252d_high
        FROM snapshot_iv
    )
    SELECT ts, avg_iv, iv_252d_low, iv_252d_high
    FROM windowed
    ORDER BY ts DESC
    LIMIT 1
""")

_UPDATE_QUERY = text("""
    UPDATE underlying_quotes
    SET iv_rank = :rank
    WHERE symbol = :symbol
      AND ts = (
          SELECT MAX(ts) FROM underlying_quotes WHERE symbol = :symbol
      )
""")

def compute_iv_rank(symbol: str, engine) -> float | None:
    with engine.connect() as conn:
        row = conn.execute(_RANK_QUERY, {"symbol": symbol}).fetchone()

    if not row:
        logger.warning("%s: no IV data found in option_quotes", symbol)
        return None

    ts, avg_iv, iv_low, iv_high = row

    if iv_high is None or iv_low is None or (iv_high - iv_low) < 0.001:
        rank = 50.0
        logger.info("%s: insufficient history for reliable IV rank — defaulting to 50.0", symbol)
    else:
        rank = ((avg_iv - iv_low) / (iv_high - iv_low)) * 100.0
        rank = max(0.0, min(100.0, rank))

    with engine.begin() as conn:
        conn.execute(_UPDATE_QUERY, {"rank": rank, "symbol": symbol})

    logger.info("%s: IV rank = %.1f  (current avg IV=%.4f)", symbol, rank, avg_iv)
    return rank

def run_iv_rank_computation(symbols: list[str] | None = None) -> dict[str, float | None]:
    symbols = symbols or WATCHLIST
    engine = create_engine(DB_URL)
    logger.info("Running IV rank computation for %d symbols", len(symbols))

    results = {}
    for sym in symbols:
        try:
            results[sym] = compute_iv_rank(sym, engine)
        except Exception as exc:
            logger.error("%s: IV rank failed — %s", sym, exc)
            results[sym] = None

    computed = sum(1 for v in results.values() if v is not None)
    logger.info("IV rank complete — %d/%d symbols updated", computed, len(symbols))
    return results

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    run_iv_rank_computation()