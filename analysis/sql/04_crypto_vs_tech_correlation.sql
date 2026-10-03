-- Q4. Has crypto started moving like a tech stock?
-- Correlation of WEEKLY log returns with QQQ, per calendar year. Weekly because
-- crypto trades 7 days and equities 5: same-day pairs would miss weekend moves.
-- (Weeks start Monday; crypto's weekend falls in the week it happens, while QQQ
-- picks it up on Monday's open - a small, consistent misalignment.)
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

select year(week)                as year,
       count(*)                  as weeks,
       round(corr(btc, qqq), 2)  as btc_vs_qqq,
       round(corr(eth, qqq), 2)  as eth_vs_qqq,
       round(corr(btc, eth), 2)  as btc_vs_eth
from pivoted
where btc is not null and eth is not null and qqq is not null
group by 1
order by 1;

-- The weekly rolling version for the dashboard is 07_export_rolling_correlation.sql.
