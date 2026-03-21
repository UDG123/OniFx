-- ==========================================================================
-- OniQuant v6.0 — FINRA/CME Compliance Views (2026)
-- ==========================================================================

-- Ensure NUMERIC precision for fractional size reporting (FINRA Feb 2026)
ALTER TABLE signal_ledger
    ADD COLUMN IF NOT EXISTS order_qty NUMERIC(18, 8),
    ADD COLUMN IF NOT EXISTS submitter VARCHAR(64),
    ADD COLUMN IF NOT EXISTS manual_order BOOLEAN DEFAULT FALSE;

-- FINRA OATS/CAT Compliance View
CREATE OR REPLACE VIEW v_compliance_log AS
SELECT
    ts                                          AS "ExecutionTimestamp",
    asset_symbol                                AS "Symbol",
    CASE signal_direction
        WHEN 1 THEN 'BUY'
        WHEN -1 THEN 'SELL'
        ELSE 'FLAT'
    END                                         AS "Side",
    order_qty                                   AS "OrderQuantity",
    target_price                                AS "OrderPrice",
    simulated_fill_price                        AS "ExecutionPrice",
    fill_latency_ms                             AS "LatencyMs",
    desk_id                                     AS "DeskId",
    COALESCE(submitter, 'ONIQUANT_ALGO')        AS "SubmitterId",
    CASE WHEN manual_order THEN 'Y' ELSE 'N' END AS "ManualOrderIndicator",
    indicator_metadata->>'signal_id'            AS "SignalId",
    posterior_prob                               AS "ModelConfidence",
    source_stream                               AS "OrderSource"
FROM signal_ledger
WHERE signal_direction != 0
ORDER BY ts DESC;

-- PnL Explain vs Predict View
CREATE OR REPLACE VIEW v_pnl_comparison AS
SELECT
    ts,
    desk_id,
    asset_symbol,
    signal_direction,
    target_price,
    simulated_fill_price,
    posterior_prob,
    -- PnL Predict: expected profit based on posterior and historical avg win
    posterior_prob * ABS(target_price * 0.01) AS pnl_predict,
    -- PnL Explain: realized profit based on synthetic fill
    CASE
        WHEN signal_direction = 1 THEN simulated_fill_price - target_price
        WHEN signal_direction = -1 THEN target_price - simulated_fill_price
        ELSE 0
    END AS pnl_explain,
    -- Deviation
    ABS(
        (CASE
            WHEN signal_direction = 1 THEN simulated_fill_price - target_price
            WHEN signal_direction = -1 THEN target_price - simulated_fill_price
            ELSE 0
        END) - (posterior_prob * ABS(target_price * 0.01))
    ) / NULLIF(ABS(posterior_prob * ABS(target_price * 0.01)), 0) AS deviation_pct
FROM signal_ledger
WHERE simulated_fill_price IS NOT NULL
  AND posterior_prob IS NOT NULL
  AND signal_direction != 0;
