-- Coverage: every expected equity symbol (seed) has rows in silver. Catches a
-- ticker never ingested / dropped from the universe — invisible to recency.
{{ config(severity = 'warn') }}
{{ assert_entity_coverage('equity', ref('stg_equity_prices'), 'symbol') }}
