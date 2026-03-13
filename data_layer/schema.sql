-- data_layer/schema.sql (V3 starter)

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------- Events (earnings + macro) ----------
CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT,
  event_type TEXT NOT NULL,           -- 'earnings', 'fomc', 'cpi', etc.
  event_ts TIMESTAMPTZ NOT NULL,
  source TEXT,
  meta JSONB
);
CREATE INDEX IF NOT EXISTS idx_events_symbol_ts ON events(symbol, event_ts);
CREATE INDEX IF NOT EXISTS idx_events_type_ts ON events(event_type, event_ts);

-- ---------- Positions (portfolio state) ----------
CREATE TABLE IF NOT EXISTS positions (
  id BIGSERIAL PRIMARY KEY,
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  strategy TEXT,                      -- 'iron_condor', 'strangle', etc.
  opened_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  qty INT NOT NULL,
  avg_price DOUBLE PRECISION,
  mark_price DOUBLE PRECISION,
  unrealized_pnl DOUBLE PRECISION,
  meta JSONB
);
CREATE INDEX IF NOT EXISTS idx_positions_account_symbol ON positions(account_id, symbol);

-- ---------- Orders (order lifecycle) ----------
CREATE TABLE IF NOT EXISTS orders (
  id BIGSERIAL PRIMARY KEY,
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  created_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL,               -- 'pending', 'submitted', 'filled', 'canceled', 'rejected'
  order_type TEXT,                    -- 'limit', 'market', etc.
  qty INT NOT NULL,
  limit_price DOUBLE PRECISION,
  filled_qty INT NOT NULL DEFAULT 0,
  avg_fill_price DOUBLE PRECISION,
  legs JSONB,                         -- for multi-leg spreads
  meta JSONB
);
CREATE INDEX IF NOT EXISTS idx_orders_account_ts ON orders(account_id, created_ts DESC);

-- ---------- (Optional but recommended) Snapshot grouping ----------
CREATE TABLE IF NOT EXISTS snapshot_runs (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  provider TEXT NOT NULL,             -- 'schwab', 'tradier', etc.
  status TEXT NOT NULL DEFAULT 'ok',
  meta JSONB
);

CREATE TABLE IF NOT EXISTS underlying_quotes (
  snapshot_id BIGINT REFERENCES snapshot_runs(id),
  ts TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  raw JSONB
);
CREATE INDEX IF NOT EXISTS idx_underlying_symbol_ts ON underlying_quotes(symbol, ts DESC);

CREATE TABLE IF NOT EXISTS option_quotes (
  snapshot_id BIGINT REFERENCES snapshot_runs(id),
  ts TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  expiry DATE NOT NULL,
  strike DOUBLE PRECISION NOT NULL,
  option_right CHAR(1) NOT NULL,             -- C/P
  bid DOUBLE PRECISION,
  ask DOUBLE PRECISION,
  iv DOUBLE PRECISION,
  delta DOUBLE PRECISION,
  gamma DOUBLE PRECISION,
  theta DOUBLE PRECISION,
  vega DOUBLE PRECISION,
  volume BIGINT,
  open_interest BIGINT,
  raw JSONB
);
CREATE INDEX IF NOT EXISTS idx_option_symbol_expiry_ts ON option_quotes(symbol, expiry, ts DESC);
CREATE INDEX IF NOT EXISTS idx_option_snapshot_lookup
  ON option_quotes(snapshot_id, symbol, expiry, strike, option_right);