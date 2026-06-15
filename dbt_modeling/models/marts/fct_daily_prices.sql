-- Unified daily OHLCV fact. Grain: one instrument per day.
-- Star-schema fact = keys + measures. Dimension attributes (symbol, exchange,
-- asset_class) live in dim_symbol and are reached via symbol_key.
with prices as (
    select * from {{ ref('int_daily_prices') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['exchange', 'symbol', 'date_day']) }} as price_key,
    {{ dbt_utils.generate_surrogate_key(['exchange', 'symbol']) }}             as symbol_key,
    date_day,
    open,
    high,
    low,
    close,
    volume
from prices
