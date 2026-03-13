"""
Events Calendar Ingestion
Pulls earnings dates and FOMC meeting dates and writes them to the events table.
This is what makes the earnings and FOMC blocking rules in rules_gate.py actually work.
Without this file, those rules silently pass everything through.
"""

import logging
from sqlalchemy import create_engine, text
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# FOMC meeting dates for 2025-2026
# Source: federalreserve.gov — update this list annually
FOMC_DATES = [
    "2026-01-28", "2026-01-29",
    "2026-03-17", "2026-03-18",
    "2026-04-28", "2026-04-29",
    "2026-06-09", "2026-06-10",
    "2026-07-28", "2026-07-29",
    "2026-09-15", "2026-09-16",
    "2026-10-27", "2026-10-28",
    "2026-12-08", "2026-12-09",
]


def ingest_fomc_dates() -> None:
    """
    Writes FOMC meeting dates to the events table.
    FOMC dates are published quarterly by the Federal Reserve.
    Update FOMC_DATES list above each year.
    """
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        for date_str in FOMC_DATES:
            conn.execute(text("""
    INSERT INTO events (symbol, event_type, event_ts, source, meta)
    VALUES (NULL, 'fomc', :event_ts, 'federal_reserve', '{"confirmed": true}')
    ON CONFLICT DO NOTHING
"""), {"event_ts": date_str})

        conn.commit()
        logger.info(f"Ingested {len(FOMC_DATES)} FOMC dates")

def ingest_earnings(symbols: list[str], schwab_client) -> None:
    """
    Pulls next earnings date for each symbol from Schwab fundamentals
    and writes to events(symbol, event_type, event_ts, source, meta).

    Requires a valid schwab_client (OAuth already done).
    """
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        for symbol in symbols:
            try:
                # Fundamental lookup (exact response shape can vary; we'll be defensive)
                resp = schwab_client.get_instruments(symbols=[symbol], projection="fundamental")
                data = resp.json()

                instruments = data.get("instruments") or []
                if not instruments:
                    logger.warning(f"{symbol}: no instruments returned")
                    continue

                inst = instruments[0]
                fundamental = inst.get("fundamental", {}) or {}
                earnings_date = fundamental.get("nextEarningsDate")

                if not earnings_date:
                    logger.warning(f"{symbol}: no nextEarningsDate found")
                    continue

                # Insert using your table's columns
                conn.execute(text("""
                    INSERT INTO events (symbol, event_type, event_ts, source, meta)
                    VALUES (:symbol, 'earnings', :event_ts, 'schwab', '{"confirmed": true}')
                    ON CONFLICT DO NOTHING
                """), {"symbol": symbol, "event_ts": earnings_date})

                logger.info(f"{symbol}: earnings on {earnings_date}")

            except Exception as e:
                logger.error(f"{symbol}: earnings ingestion failed — {e}")

        conn.commit()




def is_earnings_within_days(symbol: str, days: int) -> bool:
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        count = conn.execute(text("""
            SELECT COUNT(*) FROM events
            WHERE symbol = :symbol
              AND event_type = 'earnings'
              AND event_ts BETWEEN NOW() AND NOW() + (:days || ' days')::interval
        """), {"symbol": symbol, "days": days}).scalar()
        return count > 0


def is_fomc_within_days(days: int) -> bool:
    """
    Returns True if there is an FOMC meeting within the next N days.
    """
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        count = conn.execute(text("""
            SELECT COUNT(*) FROM events
            WHERE event_type = 'fomc'
              AND event_ts BETWEEN NOW() AND NOW() + (:days || ' days')::interval
        """), {"days": days}).scalar()
        return count > 0



if __name__ == "__main__":
    ingest_fomc_dates()
    print("FOMC dates ingested. Check your events table in TablePlus.")
    print("Run provider.py separately to authenticate with Schwab.")