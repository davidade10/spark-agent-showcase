"""
execution/router.py — Execution routing façade.

Currently delegates to execution.executor.execute_approved_candidate, which
builds the Schwab-ready iron condor payload, inserts into orders, and routes
to the paper dry_run path (or raises for live until enabled).

This module exists so higher layers can depend on a stable routing interface
without knowing about the executor's internal details.
"""
from __future__ import annotations

from execution.executor import execute_approved_candidate


def route_candidate_to_broker(candidate_id: int) -> int:
    """
    Route an approved candidate to the broker (or paper simulator).

    Returns the created order_id on success.
    """
    return execute_approved_candidate(candidate_id)

