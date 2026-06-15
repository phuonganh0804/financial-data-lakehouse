-- Per-series recency for FRED. Threshold by cadence + RELEASE LAG: FRED dates a
-- value at its period START but publishes it much later, so the newest available
-- observation can be far older than one period. Catches a single series (incl. a
-- single daily series) stalling — invisible to table-level source freshness.
-- Warn-only (gold forward-fills the last value).
{{ config(severity = 'warn') }}
{{ assert_entity_recency(
    ref('stg_fred_macro'),
    'series_id',
    "case frequency when 'daily' then 7 when 'monthly' then 75 when 'quarterly' then 210 else 75 end",
    extra_cols=['frequency']
) }}
