-- Conformed calendar dimension: every date present in any fact.
with all_dates as (
    select date_day from {{ ref('stg_binance_klines') }}
    union
    select date_day from {{ ref('stg_equity_prices') }}
    union
    select date_day from {{ ref('stg_fred_macro') }}
)

select
    date_day,
    year(date_day)         as calendar_year,
    month(date_day)        as calendar_month,
    day_of_month(date_day) as calendar_day,
    quarter(date_day)      as calendar_quarter,
    day_of_week(date_day)  as day_of_week
from all_dates
