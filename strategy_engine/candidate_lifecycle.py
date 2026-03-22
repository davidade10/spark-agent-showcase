"""
strategy_engine/candidate_lifecycle.py

Candidate status management — expiry and lifecycle transitions.

Functions:
  mark_expired_candidates(conn) -> int
      Mark approved candidates that have passed the staleness limit as
      expired. Safe to call repeatedly — idempotent on already-expired rows.

  run_mark_expired_candidates() -> int
      Scheduler entry point: creates its own DB connection and calls
      mark_expired_candidates(). Used by main.py.
"""
from __future__ import annotations

import logging
from datetime import timedelta

from sqlalchemy import create_engine, text

from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    APPROVAL_STALENESS_LIMIT_SECONDS,
)

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def mark_expired_candidates(conn) -> int:
    """
    Mark approved candidates older than APPROVAL_STALENESS_LIMIT_SECONDS as expired.

    Finds trade_candidates rows where:
      - gate_result = 'approved'
      - llm_card->>'approval_status' is not already terminal
        ('approved', 'working', 'stale', 'expired', 'rejected')
      - created_at < NOW() - APPROVAL_STALENESS_LIMIT_SECONDS

    Updates using jsonb_set so all existing llm_card fields are preserved.
    Handles NULL llm_card safely via COALESCE.

    Returns the number of rows marked expired.
    Never raises — wraps execution in try/except, logs errors, returns 0 on failure.
    """
    try:
        threshold = timedelta(seconds=APPROVAL_STALENESS_LIMIT_SECONDS)
        result = conn.execute(text("""
            UPDATE trade_candidates
            SET llm_card = jsonb_set(
                COALESCE(llm_card, '{}'::jsonb),
                '{approval_status}',
                '"expired"'
            )
            WHERE gate_result = 'approved'
              AND COALESCE(llm_card->>'approval_status', '') NOT IN (
                  'approved', 'working', 'stale', 'expired', 'rejected'
              )
              AND created_at < NOW() - :threshold
        """), {"threshold": threshold})
        count = result.rowcount
        if count:
            logger.info("mark_expired_candidates: marked %d candidate(s) as expired", count)
        return count
    except Exception as exc:
        logger.error("mark_expired_candidates: failed — %s", exc)
        return 0


def run_mark_expired_candidates() -> int:
    """
    Scheduler entry point.

    Creates its own DB engine and connection, then calls
    mark_expired_candidates(conn). Intended for use by main.py's
    job_expire_candidates() scheduler job.

    Returns the number of rows marked expired (0 on any failure).
    """
    try:
        engine = create_engine(DB_URL)
        with engine.begin() as conn:
            return mark_expired_candidates(conn)
    except Exception as exc:
        logger.error("run_mark_expired_candidates: DB connection failed — %s", exc)
        return 0
