-- Forward-fill each FRED series onto the daily price calendar: for every day a
-- price exists, carry the most recent macro observation on or before it. This
-- aligns low-frequency series (monthly CPI, quarterly GDP) — and weekend gaps
-- (daily rates have no Sat/Sun observation, but crypto trades then) — with the
-- daily returns they're reported against. Without this, a date join leaves
-- macro NULL on every day that isn't an exact observation date.
with price_dates as (
    select date_day from {{ ref('stg_binance_klines') }}
    union
    select date_day from {{ ref('stg_equity_prices') }}
),

series as (
    select distinct series_id from {{ ref('stg_fred_macro') }}
),

-- One row per (day, series) we want a value for.
grid as (
    select d.date_day, s.series_id
    from price_dates d
    cross join series s
),

observed as (
    select date_day, series_id, value
    from {{ ref('stg_fred_macro') }}
),

joined as (
    select
        g.date_day,
        g.series_id,
        o.value
    from grid g
    left join observed o
        on  o.series_id = g.series_id
        and o.date_day  = g.date_day
)

select
    date_day,
    series_id,
    -- Carry the last known value forward across days with no observation.
    last_value(value) ignore nulls over (
        partition by series_id
        order by date_day
        rows between unbounded preceding and current row
    ) as value
from joined
