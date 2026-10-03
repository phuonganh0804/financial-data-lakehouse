-- Q3. How did returns and risk differ across Fed rate regimes?
-- Regimes come from the Fed funds (DFF) month-end path in the data:
--   zero rates  2021-01 .. 2022-02   (~0.08%)
--   hiking      2022-03 .. 2023-07   (0.08% -> 5.33%)
--   plateau     2023-08 .. 2024-08   (5.33%)
--   cutting     2024-09 .. 2025-12   (5.33% -> 3.64%)
--   hold        2026-01 .. 2026-09   (~3.63%)
-- MONTHLY returns, not daily: the macro data is monthly at best, and daily rows
-- would just repeat the same rate ~21 times and overstate the sample size.
-- Groups are equal-weighted portfolios (average of member returns each month).
with monthly as (
    select symbol, asset_class, is_benchmark,
           date_trunc('month', date_day)  as month,
           exp(sum(log_return)) - 1       as monthly_return
    from financial_data_lakehouse_gold.mart_returns_vs_macro
    where date_day between date '2021-01-01' and date '2026-09-30'
      and log_return is not null
    group by 1, 2, 3, 4
),

portfolios as (
    select month,
           case when is_benchmark          then 'QQQ (benchmark)'
                when asset_class = 'equity' then '8 tech stocks'
                else 'Crypto (BTC+ETH)' end as asset_group,
           avg(monthly_return)              as portfolio_return
    from monthly
    group by 1, 2
),

tagged as (
    select *,
           case when month < date '2022-03-01' then '1 Zero rates (2021-01..2022-02)'
                when month < date '2023-08-01' then '2 Hiking (2022-03..2023-07)'
                when month < date '2024-09-01' then '3 Plateau (2023-08..2024-08)'
                when month < date '2026-01-01' then '4 Cutting (2024-09..2025-12)'
                else                                '5 Hold (2026-01..2026-09)' end as regime
    from portfolios
)

select regime,
       asset_group,
       count(*)                                                        as months,
       round((exp(sum(ln(1 + portfolio_return))) - 1) * 100, 1)        as regime_return_pct,
       round((power(exp(sum(ln(1 + portfolio_return))), 12.0 / count(*)) - 1) * 100, 1)
                                                                       as annualised_return_pct,
       round(stddev_samp(portfolio_return) * sqrt(12) * 100, 1)        as ann_vol_pct,
       round(avg(case when portfolio_return > 0 then 100.0 else 0 end), 0) as pct_up_months
from tagged
group by 1, 2
order by 1, 2;
