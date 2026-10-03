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
),

-- Benchmarks (e.g. QQQ) are flagged in the Terraform universe configs and
-- arrive via the expected_entities seed, so they can be compared against
-- rather than averaged into an asset class.
benchmarks as (
    select source, entity_id, cast(is_benchmark as boolean) as is_benchmark
    from {{ ref('expected_entities') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['i.exchange', 'i.symbol']) }} as symbol_key,
    i.symbol,
    i.exchange,
    i.asset_class,
    i.currency,
    coalesce(b.is_benchmark, false) as is_benchmark
from instruments i
left join benchmarks b
    on b.source = i.asset_class and b.entity_id = i.symbol
