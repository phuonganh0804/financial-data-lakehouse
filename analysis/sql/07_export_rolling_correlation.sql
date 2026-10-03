-- Dashboard export: rolling 26-week (half-year) correlation of weekly returns
-- with QQQ, one row per week. Same weekly construction as query 04.
with weekly as (
    select symbol,
           date_trunc('week', date_day) as week,
           sum(log_return)              as weekly_log_return
    from financial_data_lakehouse_gold.mart_returns_vs_macro
    where symbol in ('BTCUSDT', 'ETHUSDT', 'QQQ')
      and date_day between date '2021-01-01' and date '2026-09-30'
      and log_return is not null
    group by 1, 2
),

pivoted as (
    select week,
           max(case when symbol = 'BTCUSDT' then weekly_log_return end) as btc,
           max(case when symbol = 'ETHUSDT' then weekly_log_return end) as eth,
           max(case when symbol = 'QQQ'     then weekly_log_return end) as qqq
    from weekly
    group by week
)

select * from (
select week,
       round(corr(btc, qqq) over (order by week rows between 25 preceding and current row), 3) as btc_vs_qqq_26w,
       round(corr(eth, qqq) over (order by week rows between 25 preceding and current row), 3) as eth_vs_qqq_26w,
       count(*) over (order by week rows between 25 preceding and current row) as weeks_in_window
from pivoted
where btc is not null and eth is not null and qqq is not null
)
-- Drop the first 25 weeks, whose windows are partial (2 weeks gives a meaningless 1.0).
where weeks_in_window = 26
order by week;
