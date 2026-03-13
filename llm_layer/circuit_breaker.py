"""
llm_layer/circuit_breaker.py
Tracks LLM performance and trips to rules-gate-only mode on repeated failures.

State is persisted to a JSON file (not the DB) so the circuit breaker
works even if the database is having issues — which is exactly when you
need it most.

Trip conditions (either triggers a trip):
  - 3 consecutive validation failures
  - 5 or more failures in the last 10 attempts

In TRIPPED state:
  - generate_one() / generate_all_pending() are skipped
  - trade_candidates are approved/blocked by rules gate only
  - approval UI shows raw scoring data with a CIRCUIT OPEN banner
  - main.py logs a WARNING on every cycle

Reset:
  - Manual only: python -m llm_layer.circuit_breaker --reset
  - Or call circuit_breaker.reset() from code
  - Auto-reset is intentionally NOT implemented — you should investigate
    before re-enabling the LLM layer

Usage:
  from llm_layer.circuit_breaker import CircuitBreaker
  cb = CircuitBreaker()

  # Before calling Ollama:
  if cb.is_open():
      logger.warning("Circuit open — skipping LLM card generation")
      return

  # After a successful card:
  cb.record_success()

  # After a validation failure:
  cb.record_failure("validate_trade_card raised: missing field 'recommendation'")

  # Status summary for logging:
  print(cb.status_line())
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

STATE_FILE           = Path("llm_layer/circuit_breaker_state.json")
CONSECUTIVE_FAIL_TRIP = 3     # trip after N consecutive failures
WINDOW_SIZE          = 10     # rolling window for rate-based trip
WINDOW_FAIL_TRIP     = 5      # trip if >= N failures in last WINDOW_SIZE attempts


# ── State schema ─────────────────────────────────────────────────────────────
#
# {
#   "state":               "closed" | "open",
#   "consecutive_failures": int,
#   "recent_results":      list of "ok" | "fail"  (last WINDOW_SIZE),
#   "total_attempts":      int,
#   "total_failures":      int,
#   "last_failure_reason": str | null,
#   "tripped_at":          ISO timestamp | null,
#   "tripped_reason":      str | null,
#   "last_updated":        ISO timestamp,
# }


class CircuitBreaker:
    """
    Persistent circuit breaker for the LLM layer.

    State is loaded from disk on init and written back on every
    record_success() / record_failure() call. Thread-safety is not
    required — main.py runs single-threaded.
    """

    def __init__(self, state_file: Path = STATE_FILE):
        self._path = state_file
        self._state = self._load()

    # ── Persistence ──────────────────────────────────────────────────────────

    def _load(self) -> dict:
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text())
                # Ensure recent_results is a deque for easy window management
                data["recent_results"] = deque(
                    data.get("recent_results", []),
                    maxlen=WINDOW_SIZE,
                )
                return data
            except Exception as e:
                logger.warning(f"Circuit breaker state file corrupt — resetting. ({e})")

        return self._default_state()

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = dict(self._state)
        data["recent_results"] = list(self._state["recent_results"])
        data["last_updated"]   = datetime.now(timezone.utc).isoformat()
        self._path.write_text(json.dumps(data, indent=2))

    @staticmethod
    def _default_state() -> dict:
        return {
            "state":               "closed",
            "consecutive_failures": 0,
            "recent_results":      deque([], maxlen=WINDOW_SIZE),
            "total_attempts":      0,
            "total_failures":      0,
            "last_failure_reason": None,
            "tripped_at":          None,
            "tripped_reason":      None,
            "last_updated":        datetime.now(timezone.utc).isoformat(),
        }

    # ── Public interface ─────────────────────────────────────────────────────

    def is_open(self) -> bool:
        """Returns True if the circuit is tripped (LLM disabled)."""
        return self._state["state"] == "open"

    def is_closed(self) -> bool:
        """Returns True if the circuit is healthy (LLM enabled)."""
        return self._state["state"] == "closed"

    def record_success(self) -> None:
        """
        Call this after a trade card passes validate_trade_card().
        Resets consecutive failure count and appends 'ok' to the window.
        Does NOT auto-reset a tripped circuit — that requires manual reset.
        """
        s = self._state
        s["consecutive_failures"] = 0
        s["recent_results"].append("ok")
        s["total_attempts"] += 1
        self._save()
        logger.debug("Circuit breaker: success recorded.")

    def record_failure(self, reason: str) -> None:
        """
        Call this after a validation error, Ollama connection failure,
        or any exception during trade card generation.

        Automatically trips the circuit if thresholds are exceeded.
        """
        s = self._state
        s["consecutive_failures"] += 1
        s["recent_results"].append("fail")
        s["total_attempts"] += 1
        s["total_failures"]  += 1
        s["last_failure_reason"] = reason

        logger.warning(
            f"Circuit breaker: failure recorded — {reason} "
            f"(consecutive={s['consecutive_failures']}, "
            f"window={self._window_failure_count()}/{WINDOW_SIZE})"
        )

        self._check_trip()
        self._save()

    def reset(self) -> None:
        """
        Manually resets the circuit breaker to closed state.
        Clears all failure counts and trip metadata.
        Use after investigating and fixing the underlying LLM issue.
        """
        logger.info("Circuit breaker: manual reset — returning to CLOSED state.")
        self._state = self._default_state()
        self._save()

    def status_line(self) -> str:
        """One-line summary for logging and health checks."""
        s = self._state
        window_fails = self._window_failure_count()

        if self.is_open():
            return (
                f"Circuit OPEN — tripped at {s['tripped_at']} "
                f"reason='{s['tripped_reason']}' | "
                f"Run: python -m llm_layer.circuit_breaker --reset"
            )

        return (
            f"Circuit CLOSED — "
            f"consecutive_fails={s['consecutive_failures']}/{CONSECUTIVE_FAIL_TRIP} | "
            f"window={window_fails}/{WINDOW_FAIL_TRIP} of last {len(s['recent_results'])} | "
            f"total={s['total_failures']}/{s['total_attempts']} failures"
        )

    def status_dict(self) -> dict:
        """Full state dict for the approval UI or health endpoint."""
        s = self._state
        return {
            "state":               s["state"],
            "consecutive_failures": s["consecutive_failures"],
            "window_failures":     self._window_failure_count(),
            "window_size":         WINDOW_SIZE,
            "total_attempts":      s["total_attempts"],
            "total_failures":      s["total_failures"],
            "last_failure_reason": s["last_failure_reason"],
            "tripped_at":          s["tripped_at"],
            "tripped_reason":      s["tripped_reason"],
            "trip_thresholds": {
                "consecutive": CONSECUTIVE_FAIL_TRIP,
                "window":      f"{WINDOW_FAIL_TRIP}/{WINDOW_SIZE}",
            },
        }

    # ── Internal logic ───────────────────────────────────────────────────────

    def _window_failure_count(self) -> int:
        return sum(1 for r in self._state["recent_results"] if r == "fail")

    def _check_trip(self) -> None:
        """Evaluates both trip conditions and trips the circuit if either is met."""
        s = self._state
        if s["state"] == "open":
            return  # already tripped

        consecutive = s["consecutive_failures"]
        window_fails = self._window_failure_count()

        trip_reason: Optional[str] = None

        if consecutive >= CONSECUTIVE_FAIL_TRIP:
            trip_reason = (
                f"{consecutive} consecutive validation failures "
                f"(threshold={CONSECUTIVE_FAIL_TRIP})"
            )
        elif window_fails >= WINDOW_FAIL_TRIP:
            trip_reason = (
                f"{window_fails}/{len(s['recent_results'])} failures in rolling window "
                f"(threshold={WINDOW_FAIL_TRIP}/{WINDOW_SIZE})"
            )

        if trip_reason:
            s["state"]         = "open"
            s["tripped_at"]    = datetime.now(timezone.utc).isoformat()
            s["tripped_reason"] = trip_reason
            logger.error(
                f"CIRCUIT BREAKER TRIPPED — LLM layer disabled. "
                f"Reason: {trip_reason} | "
                f"Last error: {s['last_failure_reason']} | "
                f"To re-enable: python -m llm_layer.circuit_breaker --reset"
            )


# ── Convenience wrapper ──────────────────────────────────────────────────────
# Used by trade_card.py so it doesn't need to manage the CB instance itself.

_instance: Optional[CircuitBreaker] = None


def get() -> CircuitBreaker:
    """Returns the singleton CircuitBreaker instance."""
    global _instance
    if _instance is None:
        _instance = CircuitBreaker()
    return _instance


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description="Circuit breaker management")
    parser.add_argument("--reset",  action="store_true", help="Reset to closed state")
    parser.add_argument("--status", action="store_true", help="Print current status")
    args = parser.parse_args()

    cb = CircuitBreaker()

    if args.reset:
        cb.reset()
        print("Circuit breaker reset to CLOSED.")
    elif args.status:
        import json as _json
        print(_json.dumps(cb.status_dict(), indent=2))
    else:
        # Default: just print status
        print(cb.status_line())
        print()
        import json as _json
        print(_json.dumps(cb.status_dict(), indent=2))