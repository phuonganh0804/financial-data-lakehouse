-- Q2. Did each stock beat the Nasdaq-100 (QQQ), and how much did it amplify
-- the index's moves?
-- Beta = cov(stock, QQQ) / var(QQQ) on same-day daily returns: 1.5 means the
-- stock moved ~1.5% for every 1% move in the index. Window: 2021-01-01 .. 2026-09-30.
with equity as (
    select symbol, is_benchmark, date_day, daily_return, log_return
    from financial_data_lakehouse_gold.mart_returns_vs_macro
    where asset_class = 'equity'
      and date_day between date '2021-01-01' and date '2026-09-30'
      and log_return is not null
),

qqq as (
    select date_day, daily_return as qqq_return, log_return as qqq_log_return
    from equity
    where symbol = 'QQQ'
),

paired as (
    select e.symbol, e.daily_return, e.log_return, q.qqq_return, q.qqq_log_return
    from equity e
    join qqq q on q.date_day = e.date_day
    where not e.is_benchmark
)

select symbol,
       round(covar_samp(daily_return, qqq_return) / var_samp(qqq_return), 2) as beta,
       round(corr(daily_return, qqq_return), 2)                             as correlation,
       round((exp(avg(log_return) * 252) - 1) * 100, 1)                     as cagr_pct,
       round((exp(avg(qqq_log_return) * 252) - 1) * 100, 1)                 as qqq_cagr_pct,
       round(((exp(avg(log_return) * 252) - 1)
              - (exp(avg(qqq_log_return) * 252) - 1)) * 100, 1)             as excess_cagr_pp
from paired
group by symbol
order by excess_cagr_pp desc;
