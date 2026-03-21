-- ==========================================================================
-- OniQuant v6.0 — Phase 4: TimescaleDB Persistence Layer
-- ==========================================================================
-- Target: Railway.app managed PostgreSQL with TimescaleDB extension.
-- Constraint: 1-day chunk interval to prevent RAM swapping at 5-10M rows/day.
-- ==========================================================================

-- Enable TimescaleDB extension (idempotent)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ==========================================================================
-- 1. Signal Ledger — Core Trade Signal Table
-- ==========================================================================
-- Strongly typed relational columns for hot-path queries.
-- JSONB column for variable indicator metadata (GIN-indexed).
-- ==========================================================================

CREATE TABLE IF NOT EXISTS signal_ledger (
    -- Primary time dimension — all queries partition on this
    ts              TIMESTAMPTZ     NOT NULL,

    -- Desk routing identifier (e.g., "luxalgo_spx", "knn_btc", "iof_eth")
    desk_id         VARCHAR(64)     NOT NULL,

    -- Instrument symbol (e.g., "SPY", "BTC/USD", "ES_F")
    asset_symbol    VARCHAR(32)     NOT NULL,

    -- Signal polarity: 1 = LONG, -1 = SHORT, 0 = FLAT/CLOSE
    signal_direction SMALLINT       NOT NULL CHECK (signal_direction IN (-1, 0, 1)),

    -- Model's predicted target price for the signal
    target_price    DECIMAL(18, 8)  NOT NULL,

    -- Signal strength / confidence score from the originating model [0.0, 1.0]
    confidence      DECIMAL(5, 4)   DEFAULT 0.0,

    -- Entry price at signal generation time (for slippage tracking)
    entry_price     DECIMAL(18, 8),

    -- Variable indicator state — kNN neighbors, LuxAlgo confluence,
    -- Spline quantile bands, IOF flow imbalance, etc.
    -- Schema-on-read: each model writes its own key structure.
    indicator_metadata JSONB        DEFAULT '{}'::jsonb,

    -- Ingestion tracking
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_stream   VARCHAR(64)     DEFAULT 'oniquant:raw_signals'
);

-- ==========================================================================
-- 2. Convert to Hypertable — 1-Day Chunk Interval
-- ==========================================================================
-- 1-day chunks at 5-10M rows/day ≈ 5-10M rows per chunk.
-- This keeps each chunk's B-tree index resident in L2/L3 cache on Railway's
-- 8GB instances, avoiding swap thrashing during bulk COPY ingestion.
-- ==========================================================================

SELECT create_hypertable(
    'signal_ledger',
    by_range('ts', INTERVAL '1 day'),   -- 1-day chunk interval (critical constraint)
    if_not_exists => TRUE
);

-- ==========================================================================
-- 3. Indexes — Optimized for Hot-Path Query Patterns
-- ==========================================================================

-- Composite index for desk+symbol lookups within time ranges
-- Covers: "Show me all LuxAlgo SPY signals in the last hour"
CREATE INDEX IF NOT EXISTS idx_ledger_desk_symbol_ts
    ON signal_ledger (desk_id, asset_symbol, ts DESC);

-- GIN index on JSONB for arbitrary indicator metadata queries
-- Covers: "Find signals where kNN confidence > 0.8" via @> operator
CREATE INDEX IF NOT EXISTS idx_ledger_metadata_gin
    ON signal_ledger USING GIN (indicator_metadata jsonb_path_ops);

-- Partial index for active (non-flat) signals only — skip 0s
CREATE INDEX IF NOT EXISTS idx_ledger_active_signals
    ON signal_ledger (ts DESC, asset_symbol)
    WHERE signal_direction != 0;

-- ==========================================================================
-- 4. Continuous Aggregate — 1-Minute OHLCV Candles
-- ==========================================================================
-- materialized_only = false: queries transparently merge materialized data
-- with real-time unmaterialized rows from the hypertable. This gives us
-- up-to-the-second reads without waiting for the refresh policy.
-- ==========================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_signals_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 minute', ts)         AS bucket,
    asset_symbol,
    desk_id,
    -- OHLCV-style aggregation over target_price as the "price" dimension
    FIRST(target_price, ts)             AS open,
    MAX(target_price)                   AS high,
    MIN(target_price)                   AS low,
    LAST(target_price, ts)              AS close,
    COUNT(*)                            AS volume,
    -- Signal flow balance: net direction per bucket
    SUM(signal_direction)               AS net_direction,
    -- Avg model confidence per bucket (quality metric)
    AVG(confidence)                     AS avg_confidence
FROM signal_ledger
GROUP BY bucket, asset_symbol, desk_id
WITH NO DATA;   -- don't backfill on creation; let the policy handle it

-- ==========================================================================
-- 5. Continuous Aggregate — 5-Minute OHLCV Candles
-- ==========================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_signals_5m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('5 minutes', ts)        AS bucket,
    asset_symbol,
    desk_id,
    FIRST(target_price, ts)             AS open,
    MAX(target_price)                   AS high,
    MIN(target_price)                   AS low,
    LAST(target_price, ts)              AS close,
    COUNT(*)                            AS volume,
    SUM(signal_direction)               AS net_direction,
    AVG(confidence)                     AS avg_confidence
FROM signal_ledger
GROUP BY bucket, asset_symbol, desk_id
WITH NO DATA;

-- ==========================================================================
-- 6. Continuous Aggregate Refresh Policies
-- ==========================================================================
-- end_offset: Excludes the current incomplete bucket from materialization.
-- This is CRITICAL — without it, the refresh worker and ingestion workers
-- would contend on the same rows, causing lock escalation and WAL bloat.
--
-- start_offset: How far back to look for late-arriving data.
-- schedule_interval: How often the background worker runs the refresh.
-- ==========================================================================

SELECT add_continuous_aggregate_policy('cagg_signals_1m',
    start_offset    => INTERVAL '1 hour',       -- catch late arrivals up to 1h
    end_offset      => INTERVAL '1 minute',     -- exclude current incomplete 1m bucket
    schedule_interval => INTERVAL '30 seconds',  -- refresh every 30s
    if_not_exists   => TRUE
);

SELECT add_continuous_aggregate_policy('cagg_signals_5m',
    start_offset    => INTERVAL '6 hours',      -- wider lookback for 5m rollups
    end_offset      => INTERVAL '5 minutes',    -- exclude current incomplete 5m bucket
    schedule_interval => INTERVAL '1 minute',    -- refresh every 60s
    if_not_exists   => TRUE
);

-- ==========================================================================
-- 7. Data Retention Policy — Auto-Drop Old Chunks
-- ==========================================================================
-- Keep 90 days of raw signal data. Continuous aggregates persist indefinitely
-- (they reference materialized data, not raw chunks).
-- ==========================================================================

SELECT add_retention_policy('signal_ledger',
    drop_after      => INTERVAL '90 days',
    schedule_interval => INTERVAL '1 day',
    if_not_exists   => TRUE
);

-- ==========================================================================
-- 8. Compression Policy — Reduce Storage After 7 Days
-- ==========================================================================
-- TimescaleDB native compression: ~90% reduction via gorilla + delta-delta.
-- Compress chunks older than 7 days (they're read-mostly by that point).
-- ==========================================================================

ALTER TABLE signal_ledger SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'desk_id, asset_symbol',
    timescaledb.compress_orderby = 'ts DESC'
);

SELECT add_compression_policy('signal_ledger',
    compress_after  => INTERVAL '7 days',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists   => TRUE
);
