-- Instrument dimension keyed on (exchange, symbol) via a surrogate key, so the
-- same ticker on different venues stays distinct as exchanges/currencies grow.
-- Derived from the unified price rows (int_daily_prices) so the crypto+equity
-- union lives in exactly one place.
with instruments as (
    select distinct
        symbol,
        exchange,
        asset_class,
        currency
    from {{ ref('int_daily_prices') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['exchange', 'symbol']) }} as symbol_key,
    symbol,
    exchange,
    asset_class,
    currency
from instruments
