# CLAUDE.md — Spark Agent System Reference
**Last updated:** March 18, 2026
**Purpose:** Dense technical reference for Claude Code sessions. Not a tutorial.
Read this file before touching any code. Update it after every significant fix or hardening.

---

## SYSTEM IDENTITY

**Project:** Spark Agent (codebase root: `~/spark-agent/`)
**What it is:** Substantially hardened, paper-safe autonomous iron condor options trading system.
**Stack:** Python 3.12, FastAPI (port 8000), Next.js (port 3000), TimescaleDB/PostgreSQL (port 5432), schwab-py, APScheduler, Ollama llama3.2:3b
**Operator:** MacBook Air M4. Two live Schwab accounts (Individual ...8096, Roth IRA ...5760) plus one paper account.
**Current mode:** SUPERVISED PAPER. TRADING_MODE=paper. All execution routes to paper account only.
**Startup:** `cd ~/spark-agent && ./start_sparky` (runs detached)
**Shutdown:** `./stop_sparky`
**Venv:** Always activate before any Python command: `source .venv/bin/activate`
**Package manager:** uv. Use `uv run` prefix for all Python execution, not bare `python`.
**Tests:** `uv run pytest tests/ -v` — 48 tests, must all pass after any change.

---

## DIRECTORY STRUCTURE

```
spark-agent/
├── CLAUDE.md                          # this file
├── .env                               # TRADING_MODE, DB creds, account config
├── token.json                         # Schwab OAuth token — expires every 7 days
├── main.py                            # APScheduler loop — all scheduled jobs
├── start_sparky                       # pre-flight checks + boots all 3 services
├── stop_sparky                        # graceful shutdown via lsof on ports 3000/8000
│
├── data_layer/
│   ├── collector.py                   # 15-min Schwab options chain collection
│   ├── reconciler.py                  # Schwab position sync — single source of truth
│   ├── provider.py                    # Schwab OAuth2 auth, token refresh/storage
│   ├── iv_rank.py                     # daily IV rank computation (runs 4:30 PM ET)
│   ├── freshness.py                   # staleness monitoring, token health checks
│   └── events_calendar.py            # earnings + FOMC event ingestion
│
├── strategy_engine/
│   ├── candidates.py                  # iron condor candidate generation from option_quotes
│   ├── scoring.py                     # 0-100 composite score (IVR, DTE, credit/width, delta, events)
│   ├── rules_gate.py                  # hard safety filters — non-negotiable pre-LLM
│   └── exit_monitor.py               # 7-rule exit signal generator — runs after each collection
│
├── llm_layer/
│   ├── trade_card.py                  # sends candidates to Ollama llama3.2:3b for qualitative card
│   ├── circuit_breaker.py             # trips after consecutive LLM failures — fallback to rules-only
│   └── validator.py                   # Pydantic validation of LLM JSON output
│
├── execution/
│   ├── executor.py                    # order routing — paper branch active, live branch NotImplementedError
│   ├── dry_run.py                     # paper fill simulation at mid-price
│   └── order_state.py                # CANONICAL DDL OWNER for orders/positions/exit_signals/reconciler_state
│
├── approval_ui/
│   ├── api.py                         # FastAPI backend — all endpoints (port 8000)
│   └── web/app/page.tsx              # Next.js dashboard (port 3000)
│
├── tests/
│   ├── test_occ_parsing.py            # 9 tests — OCC symbol parser, fractional strikes
│   ├── test_group_reconstruction.py   # 9 tests — condor grouping, non-condor strategies
│   ├── test_closure_safety.py         # 16 tests — parser health check, 3-strike system
│   ├── test_api_positions.py          # 7 tests — /positions response shape, credit mapping
│   └── test_api_nav.py               # 6 tests — reconciler.log parsing, fallbacks
│
└── logs/
    ├── agent.log                      # main scheduler loop
    ├── backend.log                    # FastAPI
    ├── frontend.log                   # Next.js
    ├── reconciler.log                 # JSON lines — NAV and position sync data
    └── daily/                         # date-stamped EOD audit reports
```

---

## DATABASE SCHEMA — 10 TABLES (TimescaleDB)

**Connection:** localhost:5432, db: postgres, user: postgres

```sql
snapshot_runs          -- each data collection cycle
  id, ts, status, symbols_collected

option_quotes          -- raw option pricing from Schwab (hypertable on ts)
  ts, symbol, strike, expiry, option_right, bid, ask, delta,
  gamma, theta, vega, volume, open_interest, underlying_price

underlying_quotes      -- stock prices
  ts, symbol, price, iv

events                 -- earnings, FOMC, economic releases
  symbol, event_type, event_date

trade_candidates       -- generated iron condor candidates
  id, snapshot_id (FK→snapshot_runs), symbol, expiry, strikes,
  score, candidate_json, llm_card, approval_status, created_at
  approval_status values: pending | approved | rejected | stale | blocked | expired

trade_decisions        -- operator approve/reject decisions
  candidate_id (FK→trade_candidates), decision, decided_at

trade_outcomes         -- final P&L after position closes
  candidate_id, entry_credit, exit_debit, pnl

positions              -- ALL open positions (single source of truth)
  id, symbol, strategy, account_id, fill_credit, qty, status,
  position_key, source, legs_json, mark, unrealized_pnl,
  last_seen_in_schwab, strike_reset_count
  strategy values: IRON_CONDOR | SHORT_OPTION | LONG_OPTION |
                   VERTICAL_SPREAD | EQUITY | STRANGLE | STRADDLE | UNKNOWN

exit_signals           -- alerts when positions hit targets/stops
  id, position_id (FK→positions), reason, severity, status, account_id, created_at
  reason values: PROFIT_TARGET | STRONG_CLOSE | APPROACHING_STOP |
                 STOP_LOSS | TIME_EXIT_WARN | TIME_EXIT_CRITICAL | GAMMA_RISK

orders                 -- order lifecycle tracking
  id, candidate_id, status, fill_details, created_at
  status values: pending | filled | rejected | partial_cancelled
```

**DDL ownership:** `execution/order_state.py` owns ALL CREATE TABLE statements for
orders, positions, exit_signals, reconciler_state. Never duplicate DDL elsewhere.

---

## KEY ARCHITECTURAL DECISIONS

### Transaction Isolation — exit_monitor.py
Mark/P&L persistence and alert signal insertion are intentionally decoupled into two
separate transactions using savepoints:
- **Transaction 1:** Persist mark and unrealized_pnl to positions table. This must never fail.
- **Transaction 2 (savepoint):** Insert into exit_signals. If this fails, it rolls back the
  savepoint only — the pricing update in Transaction 1 is preserved.
- **Why:** A bad signal write (missing account_id, schema mismatch, duplicate) was
  previously aborting the entire pricing loop and leaving positions unpriced.
- **Do not collapse these into a single transaction.** That is a regression.

### 3-Strike Closure Safety — reconciler.py
Positions are never closed on a single reconciler run. A position must be absent from
Schwab's API response on 3 consecutive reconciler runs before the DB marks it closed.
- Tracks `last_seen_in_schwab` timestamp and a strike counter per position.
- Prevents false closures from transient Schwab API hiccups or partial responses.
- `strike_reset_count` in positions table tracks how many times the counter reset.

### Parser Health Check — reconciler.py
Before executing any closure writes, the reconciler checks what percentage of Schwab
position legs it successfully parsed into recognized OCC symbols.
- If parsed legs < 50% of total legs seen: **all closure writes are blocked.**
- The system logs a PARSER_HEALTH_FAILURE and alerts but does not close positions.
- This prevents mass false-closures when Schwab returns malformed or unexpected data.
- A parser health failure is not the same as positions being closed — it means the safety
  system activated. Always check this distinction before diagnosing data loss.

### Portfolio-Aware Collector — data_layer/collector.py
The collector runs `_load_required_contracts()` before processing each options chain.
- Reads all open option-based positions from the positions table.
- Builds a `required_contracts` set of exact tuples: (symbol, expiry, option_right, strike).
- Force-includes these specific contracts in the database regardless of DTE filter window.
- **Why:** Without this, positions near expiry (DTE < 21) or far out (DTE > 50) fell outside
  the candidate generation window and were never stored, leaving those positions unpriced.
- This fix achieved 16/16 open position pricing. Removing it is a regression.

### legs_json Parsing Fallback — reconciler.py, exit_monitor.py
legs_json is stored as a PostgreSQL JSONB field but was historically written as Python
repr strings (single quotes) rather than strict JSON. The parser uses a safe fallback:
1. Check if value is already a native list/dict (already parsed by Psycopg2)
2. Attempt `json.loads()`
3. Fall back to `ast.literal_eval()` catching ValueError, SyntaxError, TypeError
- Never remove the ast.literal_eval fallback — legacy rows still exist in the database.

### Strategy Generalization — reconciler.py
Reconciler recognizes 8 strategy types from raw Schwab OCC symbols:
IRON_CONDOR, SHORT_OPTION, LONG_OPTION, VERTICAL_SPREAD, EQUITY, STRANGLE,
STRADDLE, UNKNOWN.
- EQUITY_LIKE_TYPES set handles: ETF, CLOSED_END_FUND, MUTUAL_FUND asset types.
- position_key includes account_id for equity positions to prevent cross-account
  deduplication collisions (META equity in 8096 and 5760 are different positions).

### Stale-Data Enforcement on Approvals — approval_ui/api.py
POST /candidates/{id}/approve checks candidate freshness before allowing execution:
- Priority 1: snapshot_run timestamp linked via snapshot_id FK → snapshot_runs.ts
- Priority 2 fallback: candidate's own created_at timestamp
- If resolved timestamp > 1200 seconds (20 min): returns HTTP 422, blocks approval.
- If no timestamp found: fails closed, blocks approval.
- Never relax this to allow stale approvals. The staleness limit is intentional.

### Partial Fill Detection — reconciler.py, executor.py
After order submission, reconciler correlates broker position legs against active orders.
- Incomplete leg sets matching a recent order are quarantined rather than treated as
  normal open positions.
- Correlation uses a recent-order window and source-aware filtering to avoid false
  positives from stale orders or live_dry_run rows.

---

## API ENDPOINTS — approval_ui/api.py (port 8000)

```
GET  /health              system health, token status, data freshness, circuit breaker
GET  /candidates          pending trade candidates with scores and LLM cards
GET  /positions           all open positions with strategy, DTE, mark, P&L, status
GET  /exit-signals        exit monitor alerts with severity and trigger rule
GET  /accounts            per-account NAV (8096, 5760, PAPER, COMBINED)
POST /refresh             trigger fresh snapshot and data collection cycle
POST /candidates/{id}/approve   execute (SUPERVISED PAPER: operator-confirmed only)
POST /candidates/{id}/reject    mark candidate as rejected
POST /exit-signals/{id}/acknowledge
POST /exit-signals/{id}/snooze
POST /exit-signals/{id}/dismiss
```

---

## SCHEDULED JOBS — main.py (APScheduler)

```
Every 15 min, market hours:   collector → candidates → scoring → LLM cards → exit monitor
Daily 4:30 PM ET:             iv_rank.py
Daily 9:35 AM ET:             reconciler (morning sync)
Daily 4:05 PM ET:             reconciler (EOD sync)
```

---

## EXIT SIGNAL RULES — strategy_engine/exit_monitor.py

| Signal | Trigger | Severity |
|---|---|---|
| PROFIT_TARGET | mark ≤ 50% of fill credit | info |
| STRONG_CLOSE | mark ≤ 25% of fill credit | info |
| APPROACHING_STOP | mark ≥ 200% of fill credit | warning |
| STOP_LOSS | mark ≥ 300% of fill credit | critical |
| TIME_EXIT_WARN | DTE ≤ 21 AND profit < 30% captured | info |
| TIME_EXIT_CRITICAL | DTE ≤ 7 | warning |
| GAMMA_RISK | DTE ≤ 7 regardless of P&L | critical |

EQUITY and SHORT_OPTION strategies are excluded from condor-style stop thresholds.
Equity P&L formula: `(mark - fill_credit) * quantity` — not options-style.

---

## KNOWN FRAGILE POINTS

### Schwab 502 Errors on SPY and QQQ
SPY and QQQ consistently return HTTP 502 from Schwab's options chain API.
- They remain on the watchlist but are skipped during collection.
- This is a Schwab API limitation, not a code bug.
- Do not attempt to fix this by retrying aggressively — it will not resolve and may
  trigger Schwab rate limiting.

### 7-Day Token Renewal Window
- Schwab access tokens: expire every 30 minutes (auto-refreshed by provider.py)
- Schwab refresh tokens: expire every 7 days (manual renewal required)
- Renewal command: `python -m data_layer.provider` (opens browser, requires human)
- Token file location: `~/spark-agent/token.json`
- A lapsed refresh token = full system blindness. No collection, reconciliation, or NAV.
- freshness.py monitors token state and surfaces alerts via /health endpoint.

### legs_json Single-Quote Legacy Data
Older positions in the database have legs_json stored as Python repr strings.
- Never assume legs_json is valid JSON. Always use the three-step fallback hierarchy.
- Never rewrite these rows without testing against the full existing test suite.

### OCC Symbol Parsing Edge Cases
Schwab's lightweight API returns raw OCC symbols like `META 260417C00735000`.
- Parser extracts: underlying, expiry (YYMMDD), option_right (C/P), strike (divide by 1000)
- Fractional strikes (e.g., $0.50 increments) are handled — test_occ_parsing.py covers these.
- Unknown or malformed symbols are classified as UNKNOWN strategy, never silently dropped.

### Reconciler Log Format
`logs/reconciler.log` is JSON lines (one JSON object per line), not standard log format.
- /accounts and /nav endpoints read this file directly — format changes break the UI.
- Never write plain text to this file. Always write valid JSON objects.

### LLM Circuit Breaker State
If Ollama fails repeatedly, circuit_breaker.py trips and the system switches to
rules-gate-only mode. The dashboard shows "LLM: Tripped" in System Status.
- Trading continues without the qualitative LLM card layer.
- Circuit breaker resets automatically after a cooldown period.
- Do not restart Ollama mid-session without checking circuit breaker state first.

---

## DEBUGGING PROTOCOL

**Always diagnose in this exact order. Do not skip steps.**

```
1. BROKER TRUTH
   → Run reconciler manually: uv run python -m data_layer.reconciler --now
   → Compare output against Schwab account directly
   → If reconciler errors > 0: stop here, fix reconciler before anything else

2. RECONCILER LOG
   → tail -n 50 ~/spark-agent/logs/reconciler.log
   → Check inserted/closed/updated counts vs expected
   → Check NAV values per account

3. BACKEND LOG
   → tail -n 100 ~/spark-agent/logs/backend.log
   → grep for ERROR and WARN lines
   → Identify which endpoint or service produced the error

4. DATABASE STATE
   → Connect: psql -U postgres -d postgres
   → Check positions table for unexpected status values
   → Check exit_signals for malformed rows
   → Check trade_candidates for stale approval_status values

5. FRONTEND RENDERING
   → Check browser console for JS errors
   → Verify /positions and /accounts API responses directly with curl
   → Compare API response shape against what page.tsx expects
   → Never assume a UI bug is a backend bug without checking the API response first
```

**When proposing a fix, always return:**
1. Root cause hypothesis
2. Exact file and line range responsible
3. Evidence supporting the hypothesis
4. Safest fix order (smallest change that resolves the issue)
5. Validation steps: which tests to run, which endpoints to check

---

## CURRENT OPEN POSITIONS (last confirmed — always verify live)

**8096 Individual:**
META IRON_CONDOR 2x Apr17 | NVDA IRON_CONDOR 1x Apr17 | BE IRON_CONDOR 2x Apr17
IWM IRON_CONDOR 2x Apr17 | SMH IRON_CONDOR 3x Apr17 | AAPL EQUITY 16sh | BAC EQUITY 2sh | META EQUITY 1sh | VOO EQUITY 2sh

**5760 Roth IRA:**
META EQUITY 4sh | SMCI EQUITY 100sh | STUB EQUITY 100sh
SMCI SHORT_OPTION 1x May15 (covered call) | STUB SHORT_OPTION 1x Mar20 (covered call)

**PAPER:**
IWM IRON_CONDOR 3x May01 | VOO IRON_CONDOR 3x Apr17 | SMH IRON_CONDOR 3x Apr17 

**Combined NAV (last known):** ~$34,998.86 — always fetch live from /accounts

---

## ENVIRONMENT VARIABLES — .env

```
TRADING_MODE=paper         # paper = dry_run only | live = real Schwab orders (blocked)
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=Ayomikun123
DB_NAME=postgres
PAPER_ACCOUNT_NAV=20000
APPROVAL_STALENESS_LIMIT_SECONDS=1200
```

---

## KEY CONSTANTS (check these before hardcoding values)

| Constant | Value | Location |
|---|---|---|
| Staleness limit | 1200 sec (20 min) | approval_ui/api.py |
| Max open condors | 4 | strategy_engine/rules_gate.py |
| NAV risk per position | 6% | execution/executor.py |
| Min open interest | 100 | strategy_engine/rules_gate.py |
| DTE range (candidates) | 21-50 days | strategy_engine/candidates.py |
| Delta target (short strikes) | ~0.16 | strategy_engine/scoring.py |
| Wing width | $5 | strategy_engine/candidates.py |
| Parser health threshold | 50% legs recognized | data_layer/reconciler.py |
| 3-strike closure count | 3 consecutive absences | data_layer/reconciler.py |

---

## CHANGELOG — add one line per significant change

```
2026-03-12  Phase 5 execution layer committed (executor.py, dry_run.py)
2026-03-12  order_state.py established as canonical DDL owner
2026-03-13  Reconciler: OCC parsing, parser health check, 3-strike system
2026-03-13  Strategy generalization — 8 strategy types recognized
2026-03-13  position_key includes account_id to prevent cross-account equity collisions
2026-03-13  48-test suite: OCC, grouping, closure safety, API positions, NAV parsing
2026-03-13  start_sparky/stop_sparky scripts, pre-flight checks, /health endpoint
2026-03-13  exit_monitor.py — 7 trigger rules, mark computation, wired into scheduler
2026-03-14  Transaction isolation: mark persistence decoupled from signal insertion
2026-03-14  EQUITY and SHORT_OPTION excluded from condor-style stop thresholds
2026-03-14  legs_json parsing hardened: json.loads → ast.literal_eval fallback
2026-03-14  Portfolio-aware collector: force-includes live contracts outside DTE window
2026-03-14  Achieved 16/16 open positions priced including short-option marks
2026-03-14  Stale pending queue flushed at start of each gate cycle
2026-03-14  Candidate engine checks open positions before proposing same-symbol exposure
2026-03-14  Reconciler: partial-fill detection and hardening with correlation windows
2026-03-14  Reconciler: stress tests covering phantom verticals, stale ghosts, multi-account
2026-03-15  Backend stale-data enforcement on POST /approve (422 if snapshot > 20 min)
2026-03-15  Dashboard: refresh wired end-to-end, ghost rows quarantined, hidden-row debug view
2026-03-15  ETF reconciliation: EQUITY_LIKE_TYPES handles ETF, CLOSED_END_FUND, MUTUAL_FUND
2026-03-18  CLAUDE.md created — living system reference for Claude Code sessions
```

---

*Update this file after every significant fix, hardening, or architectural change.
One line in the changelog is enough. Keep all other sections current.*