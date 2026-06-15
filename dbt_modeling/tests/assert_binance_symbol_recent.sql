-- Per-symbol recency for Binance (24/7). Catches a single crypto symbol
-- stalling — invisible to table-level source freshness. Warn, don't block gold.
{{ config(severity = 'warn') }}
{{ assert_entity_recency(ref('stg_binance_klines'), 'symbol', 3) }}
