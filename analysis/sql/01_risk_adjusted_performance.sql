-- Q1. Which assets paid best for the risk taken?
-- Window: 2021-01-01 .. 2026-09-30 (2020 is the CPI warm-up year).
-- CAGR from mean log return; volatility annualised with 252 trading days
-- (equities) or 365 (crypto, trades daily). Sharpe uses the average Fed funds
-- rate over the window as the risk-free rate. Max drawdown = worst fall from
-- a running peak in the growth-of-$1 series.
with base as (
    select symbol, asset_class, is_benchmark, date_day,
           daily_return, log_return, fed_funds_rate
    from financial_data_lakehouse_gold.mart_returns_vs_macro
    where date_day between date '2021-01-01' and date '2026-09-30'
      and log_return is not null
),

growth as (
    select *,
           exp(sum(log_return) over (
               partition by symbol order by date_day rows unbounded preceding
           )) as growth_of_1
    from base
),

drawdowns as (
    select *,
           growth_of_1 / greatest(1.0, max(growth_of_1) over (
               partition by symbol order by date_day rows unbounded preceding
           )) - 1 as drawdown
    from growth
),

stats as (
    select symbol, asset_class, is_benchmark,
           case when asset_class = 'crypto' then 365 else 252 end as periods,
           exp(sum(log_return)) - 1      as total_return,
           avg(log_return)               as mean_log_return,
           stddev_samp(daily_return)     as daily_vol,
           avg(fed_funds_rate) / 100     as risk_free,
           min(drawdown)                 as max_drawdown,
           min_by(date_day, drawdown)    as trough_date
    from drawdowns
    group by 1, 2, 3
)

select symbol,
       asset_class,
       is_benchmark,
       round(total_return * 100, 1)                                   as total_return_pct,
       round((exp(mean_log_return * periods) - 1) * 100, 1)           as cagr_pct,
       round(daily_vol * sqrt(periods) * 100, 1)                      as ann_vol_pct,
       round(((exp(mean_log_return * periods) - 1) - risk_free)
             / (daily_vol * sqrt(periods)), 2)                        as sharpe,
       round(max_drawdown * 100, 1)                                   as max_drawdown_pct,
       trough_date
from stats
order by sharpe desc;
