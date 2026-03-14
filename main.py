"""
main.py — Scheduler / Orchestrator
The entry point that runs the entire spark-agent system.

Jobs:
  1. Every 15 min during market hours → run_collection_cycle()
  2. Every 15 min always             → run_health_check()
  3. Daily at 4:30 PM ET             → run_nightly_iv_rank()
  4. Every 30 min during market hours (9:05, 9:35 … 15:35 ET) → run_scheduled_reconciliation()
  5. End of day Mon–Fri at 4:05 PM ET                          → run_scheduled_reconciliation()

Run with:
  python main.py

Keep this terminal open while markets are open.
Ctrl+C to stop.
"""

import logging
import pathlib
import time
from datetime import datetime, timezone

import pytz
import schedule
import pandas_market_calendars as mcal
from apscheduler.schedulers.background import BackgroundScheduler

from data_layer.collector           import run_collection_cycle, WATCHLIST
from data_layer.freshness           import run_health_check
from data_layer.iv_rank             import run_iv_rank_computation
from data_layer.provider            import get_schwab_client
from data_layer.reconciler          import run_scheduled_reconciliation
from strategy_engine.candidates     import scan_for_candidates
from strategy_engine.exit_monitor   import run_exit_scan
from strategy_engine.scoring        import score_candidates
from strategy_engine.rules_gate     import run_gate as run_rules_gate
from llm_layer.trade_card           import generate_one

# ── Logging setup ─────────────────────────────────────────────────────────────
pathlib.Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),                          # print to terminal
        logging.FileHandler("logs/agent.log"),            # write to log file
    ],
)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
ET = pytz.timezone("America/New_York")
NYSE = mcal.get_calendar("NYSE")


# ── Market Hours Gate ─────────────────────────────────────────────────────────
def is_market_open() -> bool:
    """
    Returns True if the NYSE is currently open for regular trading.

    Uses pandas_market_calendars which accounts for:
      - Weekends
      - Market holidays (Christmas, Thanksgiving, etc.)
      - Early close days (day before Thanksgiving, etc.)

    This is the gate that prevents the collector from making
    API calls outside market hours when chains return empty data.
    """
    now_et = datetime.now(ET)
    today  = now_et.strftime("%Y-%m-%d")

    try:
        schedule_today = NYSE.schedule(start_date=today, end_date=today)

        if schedule_today.empty:
            logger.debug("Market closed — not a trading day")
            return False

        market_open  = schedule_today.iloc[0]["market_open"].to_pydatetime()
        market_close = schedule_today.iloc[0]["market_close"].to_pydatetime()

        now_utc = datetime.now(timezone.utc)
        is_open = market_open <= now_utc <= market_close

        if not is_open:
            logger.debug(
                f"Market closed — current time {now_et.strftime('%H:%M ET')} "
                f"outside {market_open.astimezone(ET).strftime('%H:%M')}"
                f"–{market_close.astimezone(ET).strftime('%H:%M ET')}"
            )

        return is_open

    except Exception as e:
        # If the calendar check fails, default to closed for safety
        logger.error(f"Market hours check failed — defaulting to closed: {e}")
        return False


# ── Scheduled Jobs ────────────────────────────────────────────────────────────
def job_collect(client) -> None:
    """
    Job 1 — runs every 15 minutes.
    Gated by market hours — skips silently if market is closed.
    """
    if not is_market_open():
        logger.info("Collection skipped — market is closed")
        return

    logger.info("── Starting collection cycle ──────────────────")
    try:
        summary = run_collection_cycle(client)
        logger.info(
            f"Collection complete — "
            f"snapshot_id={summary['snapshot_id']} "
            f"ok={len(summary['symbols_ok'])} "
            f"partial={len(summary['symbols_partial'])} "
            f"failed={len(summary['symbols_failed'])} "
            f"contracts={summary['total_contracts']}"
        )
    except Exception as e:
        logger.error(f"Collection cycle crashed — {e}")
        return

    # ── Strategy engine ───────────────────────────────────────────────────────
    logger.info("── Running strategy engine ────────────────────")
    try:
        candidates = scan_for_candidates()
        if not candidates:
            logger.info("No iron condor candidates found in this snapshot")
            return

        logger.info(f"Found {len(candidates)} raw candidates — scoring...")
        scored = score_candidates(candidates)
        logger.info(f"Scored {len(scored)} candidates — running rules gate...")

        gate_results = run_rules_gate(scored, client)
        total_approved = sum(
            1 for results in gate_results.values()
            for r in results if r.passed
        )
        logger.info(f"Gate complete — {total_approved} approved across all accounts")
    except Exception as e:
        logger.error(f"Strategy engine crashed — {e}")
        return

    # ── LLM card generation ───────────────────────────────────────────────────
    if total_approved > 0:
        logger.info("── Generating LLM trade cards ─────────────────")
        try:
            generate_one()
        except Exception as e:
            logger.error(f"LLM card generation crashed — {e}")

    # ── Exit signal scan ──────────────────────────────────────────────────────
    logger.info("── Running exit signal scan ───────────────────")
    try:
        new_signals = run_exit_scan()
        if new_signals:
            logger.info(f"Exit scan: {len(new_signals)} new signal(s) generated")
        else:
            logger.info("Exit scan: no new signals")
    except Exception as e:
        logger.error(f"Exit signal scan crashed — {e}")


def job_health_check() -> None:
    """
    Job 2 — runs every 15 minutes regardless of market hours.
    Logs warnings if data is stale or token is expiring.
    """
    try:
        result = run_health_check()
        if not result["ok"]:
            logger.warning("Health check: system not fully healthy — see above warnings")
    except Exception as e:
        logger.error(f"Health check crashed — {e}")


def job_iv_rank() -> None:
    """
    Job 3 — runs once daily at 4:30 PM ET after market close.
    Computes 252-day IV rank for all watchlist symbols.
    IV rank becomes meaningful after ~3 months of daily collection.
    """
    logger.info("── Starting nightly IV rank computation ───────")
    try:
        run_iv_rank_computation(WATCHLIST)
        logger.info("IV rank computation complete")
    except Exception as e:
        logger.error(f"IV rank job crashed — {e}")


def job_reconciler() -> None:
    """
    Job 4 — runs on APScheduler cron (every 30 min during market hours + EOD 4:05 PM ET).
    Syncs Schwab positions to DB and appends NAV summary to reconciler.log.
    """
    try:
        run_scheduled_reconciliation()
    except Exception as e:
        logger.error(f"Reconciler job crashed — {e}")


# ── Main Entry Point ──────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  Spark Agent starting up")
    logger.info(f"  Watchlist: {WATCHLIST}")
    logger.info(f"  Time: {datetime.now(ET).strftime('%Y-%m-%d %H:%M ET')}")
    logger.info("=" * 60)

    # Authenticate with Schwab once at startup
    # All collection jobs reuse this client — token auto-refreshes
    logger.info("Authenticating with Schwab...")
    try:
        client = get_schwab_client()
        logger.info("Schwab client ready")
    except Exception as e:
        logger.error(f"Failed to authenticate with Schwab — {e}")
        logger.error("Resolve authentication error and restart main.py")
        return

    # ── Wire up the schedule ──────────────────────────────────────────────────
    # Collection: every 15 minutes (gated by market hours internally)
    schedule.every(15).minutes.do(job_collect, client=client)

    # Health check: every 15 minutes regardless of market hours
    schedule.every(15).minutes.do(job_health_check)

    # IV rank: daily at 4:30 PM ET (30 min after market close)
    schedule.every().day.at("16:30").do(job_iv_rank)

    # Reconciler: APScheduler cron (timezone-aware)
    recon_tz = "America/New_York"
    recon_scheduler = BackgroundScheduler(timezone=recon_tz)
    recon_scheduler.add_job(
        job_reconciler,
        trigger="cron",
        day_of_week="mon-fri",
        hour="9-15",
        minute="5,35",
        timezone=recon_tz,
        id="reconciler_market_hours",
    )
    recon_scheduler.add_job(
        job_reconciler,
        trigger="cron",
        day_of_week="mon-fri",
        hour="16",
        minute="5",
        timezone=recon_tz,
        id="reconciler_eod",
    )
    recon_scheduler.start()
    logger.info("Reconciler scheduler started (9:05–15:35 ET every 30 min + 16:05 ET EOD)")

    logger.info("Schedule configured:")
    logger.info("  Collection  → every 15 minutes (market hours only)")
    logger.info("  Health check→ every 15 minutes (always)")
    logger.info("  IV rank     → daily at 4:30 PM ET")
    logger.info("  Reconciler → cron 9:05–15:35 ET every 30 min, + 16:05 ET EOD (Mon–Fri)")
    logger.info("")
    logger.info("Running first cycle immediately...")

    # Run both jobs immediately on startup so you don't wait 15 minutes
    # to see if things are working
    job_health_check()
    job_collect(client)

    logger.info("Entering scheduler loop — Ctrl+C to stop")
    logger.info("")

    # ── Main loop ─────────────────────────────────────────────────────────────
    while True:
        try:
            schedule.run_pending()
            time.sleep(30)  # check every 30 seconds for pending jobs
        except KeyboardInterrupt:
            logger.info("Spark Agent stopped by user (Ctrl+C)")
            break
        except Exception as e:
            # Never let the main loop crash — log and keep running
            logger.error(f"Main loop error — {e}")
            time.sleep(30)
            continue


if __name__ == "__main__":
    main()