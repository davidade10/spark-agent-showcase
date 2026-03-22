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
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

import schedule
from apscheduler.schedulers.background import BackgroundScheduler

from data_layer.collector           import run_collection_cycle, WATCHLIST
from execution.order_state          import take_daily_snapshot
from data_layer.freshness           import run_health_check, is_market_open
from data_layer.iv_rank             import run_iv_rank_computation
from data_layer.provider            import get_schwab_client, AuthenticationRequiredError
from data_layer.reconciler          import run_scheduled_reconciliation
from strategy_engine.candidates     import scan_for_candidates
from strategy_engine.watchlist_screener import run_screener
from data_layer.notifier            import send_telegram_msg
from strategy_engine.exit_monitor         import run_exit_scan
from strategy_engine.candidate_lifecycle  import run_mark_expired_candidates
from strategy_engine.scoring        import score_candidates
from strategy_engine.rules_gate     import run_gate as run_rules_gate
from llm_layer.trade_card           import generate_one

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),   # start_sparky redirects stdout→logs/agent.log
    ],
)
logger = logging.getLogger(__name__)

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


def job_expire_candidates() -> None:
    """
    Job — runs every 15 minutes during market hours.
    Marks approved candidates older than APPROVAL_STALENESS_LIMIT_SECONDS as expired
    so they no longer appear in the approval queue.
    Candidates that expire outside market hours are caught on the next market session.
    """
    if not is_market_open():
        return
    try:
        count = run_mark_expired_candidates()
        if count:
            logger.info(f"Expired {count} stale candidate(s)")
    except Exception as e:
        logger.error(f"Candidate expiry job crashed — {e}")


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
        run_iv_rank_computation()
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


def job_daily_snapshot() -> None:
    """
    Job — runs Mon-Fri at 09:31 ET via APScheduler.
    Captures begin-of-day total_pnl for each account into daily_snapshots.
    Used to compute daily_pnl in GET /accounts.
    """
    try:
        take_daily_snapshot()
    except Exception as e:
        logger.error(f"Daily snapshot job crashed — {e}")


def job_screener() -> None:
    """
    Job 5 — runs Mon–Fri at 9:00 AM ET via APScheduler.
    Screens the extended universe for watchlist addition candidates.
    Results are stored in the API cache and surfaced via GET /screener.
    """
    logger.info("── Running watchlist screener ─────────────────")
    try:
        result = run_screener()
        passed      = result.get("passed", 0)
        filtered    = result.get("filtered_out", 0)
        skipped     = result.get("skipped", 0)
        available   = result.get("data_available", 0)
        screened    = result.get("screen_universe_size", 0)
        candidates  = result.get("candidates", [])

        logger.info(
            "Screener complete — %d passed, %d filtered, %d skipped",
            passed, filtered, skipped,
        )

        # ── Telegram summary ──────────────────────────────────────────────────
        try:
            lines = [
                f"📋 *Watchlist Screener*",
                f"Scanned: {screened} symbols ({available} with data)",
                f"Passed: {passed} | Filtered: {filtered} | Skipped: {skipped}",
            ]
            if candidates:
                lines.append("*Top candidates:*")
                for c in candidates[:3]:
                    lines.append(
                        f"  #{c['rank']} {c['symbol']} — "
                        f"IVR {c['iv_rank']} | OI {c['open_interest']:,} | "
                        f"${c['underlying_price']}"
                    )
            else:
                lines.append("No candidates passed all filters today.")
            send_telegram_msg("\n".join(lines))
        except Exception as tg_exc:
            logger.warning(f"Screener Telegram notification failed — {tg_exc}")

    except Exception as e:
        logger.error(f"Watchlist screener job crashed — {e}")
        try:
            send_telegram_msg(f"⚠️ *Screener Error* — {e}")
        except Exception:
            pass


# ── Main Entry Point ──────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  Spark Agent starting up")
    logger.info(f"  Watchlist: {WATCHLIST}")
    logger.info(f"  Time: {datetime.now(ET).strftime('%Y-%m-%d %H:%M ET')}")
    logger.info("=" * 60)

    # Authenticate with Schwab once at startup.
    # IMPORTANT: main.py can run unattended; never trigger interactive auth here.
    client = None
    auth_available = False
    last_auth_error_log = 0.0

    def _log_auth_required(msg: str) -> None:
        nonlocal last_auth_error_log
        now = time.time()
        # Reasonable cadence: once every 10 minutes max.
        if now - last_auth_error_log < 600:
            return
        last_auth_error_log = now
        logger.critical(
            "[SCHWAB-AUTH-REQUIRED] %s Manual re-auth required: run `python -m data_layer.provider` "
            "in an interactive terminal on this machine.",
            msg,
        )

    def _ensure_client_noninteractive() -> None:
        nonlocal client, auth_available
        if auth_available and client is not None:
            return
        try:
            client = get_schwab_client(interactive=False)
            auth_available = True
            logger.info("Schwab client ready (non-interactive)")
        except AuthenticationRequiredError as e:
            auth_available = False
            client = None
            _log_auth_required(str(e))
        except Exception as e:
            auth_available = False
            client = None
            _log_auth_required(f"Unexpected auth error: {e}")

    logger.info("Authenticating with Schwab (non-interactive)...")
    _ensure_client_noninteractive()

    # ── Startup Telegram notification ──────────────────────────────────────────
    try:
        n = len(WATCHLIST)
        if is_market_open():
            startup_msg = f"🚀 *Sparky Online* — market is open. Monitoring {n} symbols."
        else:
            # Default: market closed (covers after-hours, weekends, holidays).
            startup_msg = f"🚀 *Sparky Online* — market is closed. Monitoring {n} symbols."
            # Nice-to-have: if a trading session exists today and we're pre-open,
            # show minutes until open using the NYSE calendar (same source as
            # is_market_open() — no hardcoded 9:30 AM assumption).
            try:
                import pandas_market_calendars as mcal
                _nyse  = mcal.get_calendar("NYSE")
                _today = datetime.now(ET).strftime("%Y-%m-%d")
                _sched = _nyse.schedule(start_date=_today, end_date=_today)
                if not _sched.empty:
                    _open_utc = _sched.iloc[0]["market_open"].to_pydatetime()
                    _now_utc  = datetime.now(timezone.utc)
                    if _now_utc < _open_utc:
                        _mins = int((_open_utc - _now_utc).total_seconds() / 60)
                        startup_msg = (
                            f"🚀 *Sparky Online* — monitoring {n} symbols. "
                            f"Market opens in {_mins} minutes."
                        )
            except Exception:
                pass  # fallback to "market is closed" already set above
        send_telegram_msg(startup_msg)
    except Exception as e:
        logger.warning(f"Startup Telegram notification failed — {e}")

    # ── Wire up the schedule ──────────────────────────────────────────────────
    def job_collect_guarded() -> None:
        _ensure_client_noninteractive()
        if not auth_available or client is None:
            logger.warning("Collection skipped — Schwab auth unavailable")
            return
        job_collect(client)

    def job_reconciler_guarded() -> None:
        _ensure_client_noninteractive()
        if not auth_available:
            logger.warning("Reconciler skipped — Schwab auth unavailable")
            return
        job_reconciler()

    # Collection: every 15 minutes (gated by market hours internally)
    schedule.every(15).minutes.do(job_collect_guarded)

    # Candidate expiry: every 15 minutes (gated by market hours internally)
    schedule.every(15).minutes.do(job_expire_candidates)

    # Health check: every 15 minutes regardless of market hours
    schedule.every(15).minutes.do(job_health_check)

    # IV rank: daily at 4:30 PM ET (30 min after market close)
    schedule.every().day.at("16:30").do(job_iv_rank)

    # Reconciler: APScheduler cron (timezone-aware)
    recon_tz = "America/New_York"
    recon_scheduler = BackgroundScheduler(timezone=recon_tz)
    recon_scheduler.add_job(
        job_reconciler_guarded,
        trigger="cron",
        day_of_week="mon-fri",
        hour="9-15",
        minute="5,35",
        timezone=recon_tz,
        id="reconciler_market_hours",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    recon_scheduler.add_job(
        job_reconciler_guarded,
        trigger="cron",
        day_of_week="mon-fri",
        hour="16",
        minute="5",
        timezone=recon_tz,
        id="reconciler_eod",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    recon_scheduler.add_job(
        job_screener,
        trigger="cron",
        day_of_week="mon-fri",
        hour="9",
        minute="0",
        timezone=recon_tz,
        id="watchlist_screener",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    recon_scheduler.add_job(
        job_daily_snapshot,
        trigger="cron",
        day_of_week="mon-fri",
        hour="9",
        minute="31",
        timezone=recon_tz,
        id="daily_snapshot",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    recon_scheduler.start()
    logger.info("Reconciler scheduler started (9:05–15:35 ET every 30 min + 16:05 ET EOD)")

    logger.info("Schedule configured:")
    logger.info("  Collection  → every 15 minutes (market hours only)")
    logger.info("  Expiry      → every 15 minutes (market hours only)")
    logger.info("  Health check→ every 15 minutes (always)")
    logger.info("  IV rank     → daily at 4:30 PM ET")
    logger.info("  Reconciler → cron 9:05–15:35 ET every 30 min, + 16:05 ET EOD (Mon–Fri)")
    logger.info("  Screener   → daily 9:00 AM ET (Mon–Fri)")
    logger.info("")
    logger.info("Running first cycle immediately...")

    # Run both jobs immediately on startup so you don't wait 15 minutes
    # to see if things are working
    job_health_check()

    # Always perform a pricing-only mark refresh at startup using the latest
    # available snapshot, even if the market is currently closed. This updates
    # positions.mark / unrealized_pnl without generating new exit signals.
    try:
        from strategy_engine.exit_monitor import run_exit_scan as _run_exit_scan_startup
        _run_exit_scan_startup(pricing_only=True)
    except Exception as e:
        logger.error(f"Startup pricing-only exit_monitor pass failed — {e}")

    # Collection + full exit scan remain gated by market hours inside job_collect
    job_collect_guarded()

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