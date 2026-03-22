
"""
Freshness Watchdog — Data Layer
Runs every 15 minutes alongside the collector.

Answers two questions:
  1. Is my market data actually fresh, or did the collector silently fail?
  2. Is my Schwab token about to expire?

Silent failures are the most dangerous failure mode in a trading system.
A crash is obvious. Stale data that looks fine is invisible — your strategy
engine would score candidates on prices from hours ago.

This file never raises. Every check returns a result dict so the scheduler
can log and alert without crashing the main loop.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytz
import pandas_market_calendars as mcal
from sqlalchemy import create_engine, text

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, SCHWAB_TOKEN_PATH
from data_layer.provider import get_schwab_client, AuthenticationRequiredError

logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

ET = pytz.timezone("America/New_York")
NYSE = mcal.get_calendar("NYSE")


def is_market_open() -> bool:
    """
    Returns True if the NYSE is currently open for regular trading.
    Shared by main.py and approval_ui/api.py for refresh logic.
    """
    now_et = datetime.now(ET)
    today = now_et.strftime("%Y-%m-%d")
    try:
        schedule_today = NYSE.schedule(start_date=today, end_date=today)
        if schedule_today.empty:
            return False
        market_open = schedule_today.iloc[0]["market_open"].to_pydatetime()
        market_close = schedule_today.iloc[0]["market_close"].to_pydatetime()
        now_utc = datetime.now(timezone.utc)
        return market_open <= now_utc <= market_close
    except Exception as e:
        logger.error(f"Market hours check failed — defaulting to closed: {e}")
        return False


# ── Thresholds ────────────────────────────────────────────────────────────────
STALE_AFTER_MINUTES  = 20   # alert if most recent snapshot is older than this
TOKEN_WARN_DAYS      = 2    # alert if token expires within this many days
TOKEN_PATH           = Path(SCHWAB_TOKEN_PATH or os.getenv("SCHWAB_TOKEN_PATH", "token.json"))


# ── Check 1: Data Freshness ───────────────────────────────────────────────────
def check_data_freshness() -> dict:
    """
    Queries snapshot_runs for the most recent successful snapshot.
    Returns a result dict describing freshness status.

    Result dict shape:
      {
        "ok":           True/False,
        "status":       "fresh" | "stale" | "no_data",
        "last_snapshot_id":  int | None,
        "last_snapshot_ts":  datetime | None,
        "age_minutes":       float | None,
        "message":           str,
      }

    Why 20 minutes: the collector runs every 15 minutes. If the most recent
    successful snapshot is older than 20 minutes, the collector either crashed
    silently or failed to write. 20 minutes gives a 5-minute grace window
    before alerting.
    """
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT id, ts, status, meta
                FROM snapshot_runs
                WHERE status IN ('ok', 'partial')
                ORDER BY ts DESC
                LIMIT 1
            """)).fetchone()

        if not row:
            return {
                "ok":                False,
                "status":            "no_data",
                "last_snapshot_id":  None,
                "last_snapshot_ts":  None,
                "age_minutes":       None,
                "message":           "No successful snapshots found in database. "
                                     "Has the collector run yet today?",
            }

        last_ts     = row.ts
        now         = datetime.now(timezone.utc)
        age_minutes = (now - last_ts).total_seconds() / 60

        if age_minutes <= STALE_AFTER_MINUTES:
            return {
                "ok":                True,
                "status":            "fresh",
                "last_snapshot_id":  row.id,
                "last_snapshot_ts":  last_ts,
                "age_minutes":       round(age_minutes, 1),
                "message":           f"Data is fresh — last snapshot "
                                     f"{round(age_minutes, 1)} minutes ago "
                                     f"(snapshot_id={row.id})",
            }
        else:
            return {
                "ok":                False,
                "status":            "stale",
                "last_snapshot_id":  row.id,
                "last_snapshot_ts":  last_ts,
                "age_minutes":       round(age_minutes, 1),
                "message":           f"⚠ DATA STALE — last snapshot was "
                                     f"{round(age_minutes, 1)} minutes ago. "
                                     f"Collector may have crashed silently.",
            }

    except Exception as e:
        return {
            "ok":                False,
            "status":            "error",
            "last_snapshot_id":  None,
            "last_snapshot_ts":  None,
            "age_minutes":       None,
            "message":           f"Freshness check failed with error: {e}",
        }


# ── Check 2: Token Expiry ─────────────────────────────────────────────────────
def check_token_expiry() -> dict:
    """
    Reads token.json and checks how much time remains on the refresh token.
    Schwab refresh tokens last exactly 7 days from creation_timestamp.
    """
    if not TOKEN_PATH.exists():
        return {
            "ok":             False,
            "status":         "missing",
            "expires_at":     None,
            "days_remaining": None,
            "message":        f"token.json not found at {TOKEN_PATH}. "
                              f"Manual re-auth required: python -m data_layer.provider",
        }

    try:
        with open(TOKEN_PATH) as f:
            token_data = json.load(f)

        creation_ts = token_data.get("creation_timestamp")

        if not creation_ts:
            return {
                "ok":             False,
                "status":         "error",
                "expires_at":     None,
                "days_remaining": None,
                "message":        "Could not find creation_timestamp in token.json.",
            }

        # Refresh token lasts exactly 7 days from creation
        expires_at_ts  = creation_ts + (7 * 86400)
        expires_at     = datetime.fromtimestamp(expires_at_ts, tz=timezone.utc)
        now            = datetime.now(timezone.utc)
        days_remaining = (expires_at - now).total_seconds() / 86400

        if days_remaining < 0:
            return {
                "ok":             False,
                "status":         "expired",
                "expires_at":     expires_at,
                "days_remaining": round(days_remaining, 1),
                "message":        "⚠ TOKEN EXPIRED — manual re-auth required: "
                                  "python -m data_layer.provider",
            }
        elif days_remaining <= TOKEN_WARN_DAYS:
            return {
                "ok":             False,
                "status":         "expiring_soon",
                "expires_at":     expires_at,
                "days_remaining": round(days_remaining, 1),
                "message":        f"⚠ TOKEN EXPIRING in {round(days_remaining, 1)} days "
                                  f"(expires {expires_at.strftime('%Y-%m-%d %H:%M UTC')}). "
                                  f"Re-authenticate soon: python -m data_layer.provider",
            }
        else:
            result = {
                "ok":             True,
                "status":         "valid",
                "expires_at":     expires_at,
                "days_remaining": round(days_remaining, 1),
                "message":        f"Token valid — {round(days_remaining, 1)} days remaining "
                                  f"(expires {expires_at.strftime('%Y-%m-%d %H:%M UTC')})",
            }
            # Extra signal: verify non-interactive client init won't prompt.
            # This should be safe in unattended mode because interactive=False.
            try:
                get_schwab_client(interactive=False)
                result["noninteractive_ok"] = True
            except AuthenticationRequiredError:
                result["noninteractive_ok"] = False
                result["ok"] = False
                result["status"] = "auth_required"
                result["message"] = (
                    "⚠ TOKEN REAUTH REQUIRED — Schwab token cannot be refreshed non-interactively. "
                    "Run: python -m data_layer.provider"
                )
            except Exception:
                # Don't fail health check on transient issues; just omit this signal.
                result["noninteractive_ok"] = None
            return result

    except Exception as e:
        return {
            "ok":             False,
            "status":         "error",
            "expires_at":     None,
            "days_remaining": None,
            "message":        f"Token check failed with error: {e}",
        }

# ── Combined Health Check ─────────────────────────────────────────────────────
def run_health_check() -> dict:
    """
    Runs both checks and returns a combined health report.
    Called every 15 minutes by the scheduler in main.py.

    Logs warnings for anything not ok.
    Returns combined dict for the scheduler to act on.
    """
    freshness = check_data_freshness()

    token     = check_token_expiry()

    overall_ok = freshness["ok"] and token["ok"]

    if freshness["ok"]:
        logger.info(f"Freshness: {freshness['message']}")
    else:
        logger.warning(f"Freshness: {freshness['message']}")

    if token["ok"]:
        logger.info(f"Token: {token['message']}")
    else:
        logger.warning(f"Token: {token['message']}")

    return {
        "ok":        overall_ok,
        "freshness": freshness,
        "token":     token,
    }


# ── Manual test run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    print("Running freshness health check...\n")

    result = run_health_check()

    print("\n── Freshness Check ─────────────────────────")
    print(f"Status:  {result['freshness']['status']}")
    print(f"Message: {result['freshness']['message']}")
    if result['freshness']['age_minutes'] is not None:
        print(f"Age:     {result['freshness']['age_minutes']} minutes")

    print("\n── Token Check ─────────────────────────────")
    print(f"Status:  {result['token']['status']}")
    print(f"Message: {result['token']['message']}")

    print("\n── Overall ─────────────────────────────────")
    print(f"System healthy: {result['ok']}")
    print("────────────────────────────────────────────")