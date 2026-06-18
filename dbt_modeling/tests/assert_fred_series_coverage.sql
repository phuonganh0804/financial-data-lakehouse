-- Coverage: every expected FRED series (seed) has rows in silver. 
-- Catches a series never ingested / dropped from the universe.
{{ config(severity = 'warn') }}
{{ assert_entity_coverage('fred', ref('stg_fred_macro'), 'series_id') }}
