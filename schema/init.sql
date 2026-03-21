-- ==========================================================================
-- OniQuant v6.0 — TimescaleDB Schema Initialization
-- ==========================================================================
-- Run once on TimescaleDB instance startup.
-- docker-compose: mounted as /docker-entrypoint-initdb.d/init.sql
--
-- Constraints enforced:
--   • 1-day chunk interval (prevents RAM swapping at 5-10M rows/day)
--   • JSONB + GIN index for variable indicator metadata
--   • Continuous aggregates with end_offset to exclude incomplete buckets
-- ==========================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ==========================================================================
-- 1. Signal Ledger — Core Trade Signal Hypertable
-- ==========================================================================

CREATE TABLE IF NOT EXISTS signal_ledger (
    -- Primary time dimension — all partitioning keys on this
    ts                  TIMESTAMPTZ     NOT NULL,

    -- Desk routing identifier (e.g., "luxalgo_spx", "knn_btc", "iof_eth")
    desk_id             VARCHAR(64)     NOT NULL,

    -- Instrument symbol (e.g., "SPY", "BTC/USD", "ES_F")
    asset_symbol        VARCHAR(32)     NOT NULL,

    -- Signal polarity: 1 = LONG, -1 = SHORT, 0 = FLAT/CLOSE
    signal_direction    SMALLINT        NOT NULL
                        CHECK (signal_direction IN (-1, 0, 1)),

    -- Model's predicted target price for this signal
    target_price        DECIMAL(18, 8)  NOT NULL,

    -- Model confidence score [0.0, 1.0]
    confidence          DECIMAL(5, 4)   DEFAULT 0.0,

    -- Entry price at signal generation (for slippage tracking)
    entry_price         DECIMAL(18, 8),

    -- Simulated fill price from the Synthetic Matcher (NULL if unfilled)
    simulated_fill_price DECIMAL(18, 8),

    -- Matcher latency in milliseconds (NULL if unfilled)
    fill_latency_ms     DECIMAL(10, 3),

    -- Bayesian posterior probability at authorization time
    posterior_prob       DECIMAL(7, 6),

    -- Variable indicator state — kNN neighbors, LuxAlgo confluence,
    -- Spline quantile bands, IOF flow imbalance, etc.
    -- Schema-on-read: each model writes its own key structure.
    -- GIN-indexed for fast @> containment queries.
    indicator_metadata  JSONB           DEFAULT '{}'::jsonb,

    -- Ingestion tracking
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_stream       VARCHAR(64)     DEFAULT 'oniquant:raw_signals'
);

-- ==========================================================================
-- 2. Convert to Hypertable — 1-Day Chunk Interval
-- ==========================================================================
-- 1-day chunks at 5-10M rows/day keeps per-chunk B-tree indexes
-- resident in L2/L3 cache on Railway's 8GB instances.
-- Prevents swap thrashing during bulk COPY ingestion.
-- ==========================================================================

SELECT create_hypertable(
    'signal_ledger',
    by_range('ts', INTERVAL '1 day'),
    if_not_exists => TRUE
);

-- ==========================================================================
-- 3. Indexes
-- ==========================================================================

-- Composite: desk + symbol within time ranges
CREATE INDEX IF NOT EXISTS idx_ledger_desk_symbol_ts
    ON signal_ledger (desk_id, asset_symbol, ts DESC);

-- GIN on JSONB for arbitrary indicator metadata queries
-- e.g., WHERE indicator_metadata @> '{"knn_confidence": 0.85}'
CREATE INDEX IF NOT EXISTS idx_ledger_metadata_gin
    ON signal_ledger USING GIN (indicator_metadata jsonb_path_ops);

-- Partial: only active (non-flat) signals
CREATE INDEX IF NOT EXISTS idx_ledger_active_signals
    ON signal_ledger (ts DESC, asset_symbol)
    WHERE signal_direction != 0;

-- Partial: only filled signals (for performance reporting)
CREATE INDEX IF NOT EXISTS idx_ledger_filled
    ON signal_ledger (ts DESC, desk_id)
    WHERE simulated_fill_price IS NOT NULL;

-- ==========================================================================
-- 4. Continuous Aggregate — 1-Minute Signal Candles
-- ==========================================================================
-- materialized_only = false: transparently merges materialized data
-- with real-time rows for up-to-the-second reads.
-- ==========================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_signals_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 minute', ts)         AS bucket,
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
-- 5. Continuous Aggregate — 5-Minute Signal Candles
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
-- end_offset = INTERVAL '1 hour':
--   Excludes the current incomplete 1-hour window from materialization.
--   This prevents write contention between the refresh worker and
--   high-frequency ingestion workers writing to the same chunk.
--   Trade-off: aggregates lag by up to 1 hour, but raw data is always
--   available via materialized_only = false.
-- ==========================================================================

SELECT add_continuous_aggregate_policy('cagg_signals_1m',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval  => INTERVAL '1 minute',
    if_not_exists     => TRUE
);

SELECT add_continuous_aggregate_policy('cagg_signals_5m',
    start_offset      => INTERVAL '12 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval  => INTERVAL '5 minutes',
    if_not_exists     => TRUE
);

-- ==========================================================================
-- 7. Compression — Reduce Storage After 7 Days
-- ==========================================================================

ALTER TABLE signal_ledger SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'desk_id, asset_symbol',
    timescaledb.compress_orderby = 'ts DESC'
);

SELECT add_compression_policy('signal_ledger',
    compress_after    => INTERVAL '7 days',
    schedule_interval  => INTERVAL '1 hour',
    if_not_exists     => TRUE
);

-- ==========================================================================
-- 8. Retention — Drop Raw Chunks After 90 Days
-- ==========================================================================

SELECT add_retention_policy('signal_ledger',
    drop_after        => INTERVAL '90 days',
    schedule_interval  => INTERVAL '1 day',
    if_not_exists     => TRUE
);
