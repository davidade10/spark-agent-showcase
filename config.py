import os
from dotenv import load_dotenv

load_dotenv()

TRADING_MODE      = os.getenv("TRADING_MODE", "paper")
# Hard kill switch for live order submission. Must be explicitly enabled
# in addition to TRADING_MODE='live' before any Schwab POST order call.
ENABLE_LIVE_SEND  = os.getenv("ENABLE_LIVE_SEND", "false").strip().lower() in ("1", "true", "yes", "y", "on")
DB_HOST           = os.getenv("DB_HOST", "localhost")
DB_PORT           = os.getenv("DB_PORT", "5432")
DB_NAME           = os.getenv("DB_NAME", "postgres")
DB_USER           = os.getenv("DB_USER", "postgres")
DB_PASSWORD       = os.getenv("DB_PASSWORD")
LLM_HOST          = os.getenv("LLM_HOST", "http://localhost:11434")
SCHWAB_API_KEY    = os.getenv("SCHWAB_API_KEY")
SCHWAB_APP_SECRET = os.getenv("SCHWAB_APP_SECRET")
SCHWAB_CALLBACK_URL = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182/")
SCHWAB_TOKEN_PATH = os.getenv("SCHWAB_TOKEN_PATH", "token.json")
# Schwab auth interactivity:
# - "auto"  (default): interactive only when stdin is a TTY
# - "true": always allow browser/input auth flows
# - "false": never allow interactive auth; raise AuthenticationRequiredError instead
SCHWAB_AUTH_INTERACTIVE = os.getenv("SCHWAB_AUTH_INTERACTIVE", "auto").strip().lower()
TELEGRAM_BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "")
APPROVAL_STALENESS_LIMIT_SECONDS = int(os.getenv("APPROVAL_STALENESS_LIMIT_SECONDS", "1200"))

# Historical starting balance used to compute displayed PAPER NAV.
# Formula: paper_nav = PAPER_ACCOUNT_STARTING_BALANCE + SUM(trade_outcomes.pnl WHERE account_id='PAPER')
# This is NOT used for position sizing — see PAPER_ACCOUNT_NAV below for that.
PAPER_ACCOUNT_STARTING_BALANCE: float = 20000.0

# Fallback NAV values for position sizing in executor.py.
# These are used ONLY when the live GET /accounts API call fails at execution time.
# Under normal operation, executor._fetch_live_nav() fetches the current NAV dynamically.
PAPER_ACCOUNT_NAV = float(os.getenv("PAPER_ACCOUNT_NAV", "20000"))
LIVE_ACCOUNT_NAV  = float(os.getenv("LIVE_ACCOUNT_NAV",  "14836"))

HARD_RULES = {
    "max_position_risk_pct": 0.06,
"max_correlated_risk_pct": 0.15,
    "max_open_condors_live":         5,
    "max_open_condors_paper":        8,
    "min_dte":                      21,
    "max_dte":                      50,
    "max_short_delta":              0.22,
    "min_net_credit":               0.40,
    "max_spread_width":             10.0,
    "blocked_within_earnings_days": 5,
    "blocked_within_fomc_days":     2,
    "min_underlying_adv":           1_000_000,
    "daily_loss_kill_pct":          0.03,
    "min_short_strike_oi":          100,
}