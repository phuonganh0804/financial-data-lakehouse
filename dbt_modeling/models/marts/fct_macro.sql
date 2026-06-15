-- Macro observations fact. Grain: one series per day.
-- FKs: series_id -> dim_series, date_day -> dim_date. Measure: value.
select
    {{ dbt_utils.generate_surrogate_key(['series_id', 'date_day']) }} as macro_key,
    series_id,
    date_day,
    value
from {{ ref('stg_fred_macro') }}
