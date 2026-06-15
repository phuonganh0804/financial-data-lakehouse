-- FRED macro series dimension. series_id is a stable natural key (globally
-- unique in FRED), so no surrogate is needed — unlike dim_symbol, where
-- (exchange, symbol) is a composite.
select distinct
    series_id,
    series_name,
    frequency,
    unit
from {{ ref('stg_fred_macro') }}
