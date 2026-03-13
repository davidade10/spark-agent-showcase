import os
from dotenv import load_dotenv

load_dotenv()

TRADING_MODE      = os.getenv("TRADING_MODE", "paper")
DB_HOST           = os.getenv("DB_HOST", "localhost")
DB_PORT           = os.getenv("DB_PORT", "5432")
DB_NAME           = os.getenv("DB_NAME", "postgres")
DB_USER           = os.getenv("DB_USER", "postgres")
DB_PASSWORD       = os.getenv("DB_PASSWORD")
LLM_HOST          = os.getenv("LLM_HOST", "http://localhost:11434")
SCHWAB_API_KEY    = os.getenv("SCHWAB_API_KEY")
SCHWAB_APP_SECRET = os.getenv("SCHWAB_APP_SECRET")

HARD_RULES = {
    "max_position_risk_pct": 0.06,
"max_correlated_risk_pct": 0.15,
    "max_open_condors":             4,
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