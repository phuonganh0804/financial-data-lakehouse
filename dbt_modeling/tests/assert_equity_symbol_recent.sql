-- Per-symbol recency for equities (NASDAQ-gated). Looser threshold to ride out
-- weekend/holiday closures. Catches a single ticker stalling. Warn-only.
{{ config(severity = 'warn') }}
{{ assert_entity_recency(ref('stg_equity_prices'), 'symbol', 5) }}
