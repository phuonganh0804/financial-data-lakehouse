-- Coverage: every expected crypto symbol (seed) has rows in silver. 
-- Catches a symbol never ingested / dropped from the universe.
{{ config(severity = 'warn') }}
{{ assert_entity_coverage('crypto', ref('stg_binance_klines'), 'symbol') }}
