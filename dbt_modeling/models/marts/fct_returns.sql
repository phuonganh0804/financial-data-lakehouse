-- Daily and log returns plus 30-day rolling volatility. Grain: instrument per day.
-- Windows partition by symbol_key (the instrument); keys + measures only.
with prices as (
    select symbol_key, date_day, close
    from {{ ref('fct_daily_prices') }}
),

with_prev as (
    select
        symbol_key,
        date_day,
        close,
        lag(close) over (
            partition by symbol_key
            order by date_day
        ) as prev_close
    from prices
),

returns as (
    select
        symbol_key,
        date_day,
        close,
        prev_close,
        case
            when prev_close is not null and prev_close <> 0
            then close / prev_close - 1
        end as daily_return,
        case
            when prev_close is not null and prev_close > 0 and close > 0
            then ln(close / prev_close)
        end as log_return
    from with_prev
)

select
    {{ dbt_utils.generate_surrogate_key(['symbol_key', 'date_day']) }} as return_key,
    symbol_key,
    date_day,
    close,
    prev_close,
    daily_return,
    log_return,
    stddev_samp(daily_return) over (
        partition by symbol_key
        order by date_day
        rows between 29 preceding and current row
    ) as volatility_30d
from returns
