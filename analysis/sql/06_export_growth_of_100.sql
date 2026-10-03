-- Dashboard export: value of $100 invested on 2021-01-01, per asset per day.
-- Feeds the "growth of $100" line chart (assets vs. the QQQ benchmark).
select date_day,
       symbol,
       asset_class,
       is_benchmark,
       round(100 * exp(sum(log_return) over (
           partition by symbol order by date_day rows unbounded preceding
       )), 4) as value_of_100,
       fed_funds_rate
from financial_data_lakehouse_gold.mart_returns_vs_macro
where date_day between date '2021-01-01' and date '2026-09-30'
  and log_return is not null
order by symbol, date_day;
