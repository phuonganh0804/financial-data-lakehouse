-- Q5. What did investors actually earn after inflation?
-- Real return = (1 + nominal) / (CPI_end / CPI_start) - 1, using the CPI level
-- prevailing on the first and last day of each period. Periods: the 2021-22
-- inflation surge, and the whole window.
-- Note: this deflates by the CPI ratio directly instead of compounding the mart's
-- real_daily_return, which applies 1/365 of a year's inflation per ROW and so
-- under-deflates assets with only ~252 trading-day rows a year (see FINDINGS.md).
with periods as (
    select '2021-22 inflation surge' as period, date '2021-01-01' as start_day, date '2022-12-31' as end_day
    union all
    select 'Full window 2021-01..2026-09', date '2021-01-01', date '2026-09-30'
),

base as (
    select p.period,
           case when m.is_benchmark          then 'QQQ (benchmark)'
                when m.asset_class = 'equity' then '8 tech stocks'
                else 'Crypto (BTC+ETH)' end as asset_group,
           m.symbol, m.date_day, m.log_return, m.cpi
    from financial_data_lakehouse_gold.mart_returns_vs_macro m
    join periods p on m.date_day between p.start_day and p.end_day
    where m.log_return is not null
),

per_symbol as (
    select period, asset_group, symbol,
           exp(sum(log_return)) - 1                           as nominal_return,
           max_by(cpi, date_day) / min_by(cpi, date_day)       as cpi_ratio
    from base
    group by 1, 2, 3
)

select period,
       asset_group,
       round(avg(nominal_return) * 100, 1)                        as nominal_return_pct,
       round((max(cpi_ratio) - 1) * 100, 1)                       as cpi_inflation_pct,
       round(avg((1 + nominal_return) / cpi_ratio - 1) * 100, 1)  as real_return_pct
from per_symbol
group by 1, 2
order by 1, 2;
