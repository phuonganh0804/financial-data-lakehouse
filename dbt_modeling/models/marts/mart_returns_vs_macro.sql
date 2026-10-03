-- Report mart (denormalised): asset daily returns aligned with daily macro.
-- Macro comes from int_macro_daily (forward-filled), so monthly CPI and
-- quarterly GDP carry across every day until their next release, and daily
-- rates carry across weekends to align with crypto returns.
with returns as (
    select
        symbol_key, date_day, daily_return, log_return, volatility_30d,
        -- Calendar days this return covers: 1 for crypto, 3 over a weekend
        -- for equities. Used to deflate by the inflation of that whole span.
        date_diff(
            'day',
            lag(date_day) over (partition by symbol_key order by date_day),
            date_day
        ) as days_covered
    from {{ ref('fct_returns') }}
),

symbols as (
    select symbol_key, symbol, exchange, asset_class, is_benchmark
    from {{ ref('dim_symbol') }}
),

macro as (
    select
        date_day,
        max(case when series_id = 'DFF'      then value end) as fed_funds_rate,
        max(case when series_id = 'DGS10'    then value end) as treasury_yield_10y,
        max(case when series_id = 'T10YIE'   then value end) as inflation_expectation_10y,
        max(case when series_id = 'CPIAUCSL' then value end) as cpi,
        max(case when series_id = 'GDPC1'    then value end) as real_gdp
    from {{ ref('int_macro_daily') }}
    group by date_day
),

-- CPI is an index LEVEL, so the inflation RATE (needed for real returns) is
-- derived: this day's CPI vs ~one year prior. The daily grid is calendar-based
-- (crypto trades daily), so the prior-year row is ~365 days back.
macro_rates as (
    select
        m.date_day,
        m.fed_funds_rate,
        m.treasury_yield_10y,
        m.inflation_expectation_10y,
        m.cpi,
        m.real_gdp,
        m.cpi / nullif(py.cpi, 0) - 1 as cpi_yoy
    from macro m
    left join macro py on py.date_day = date_add('year', -1, m.date_day)
)

select
    r.date_day,
    s.asset_class,
    s.exchange,
    s.symbol,
    s.is_benchmark,
    r.daily_return,
    r.log_return,
    r.volatility_30d,
    m.fed_funds_rate,
    m.treasury_yield_10y,
    m.inflation_expectation_10y,
    m.cpi,
    m.real_gdp,
    m.cpi_yoy,
    -- Approximate real daily return: nominal deflated by YoY inflation for the
    -- calendar days the return covers. Deflating per ROW (1/365) would remove
    -- only ~252/365 of a year's inflation from equities. Approximate because
    -- inflation is monthly — use cpi_yoy directly for period-level analysis.
    (1 + r.daily_return) / power(1 + m.cpi_yoy, r.days_covered / 365.0) - 1 as real_daily_return
from returns r
inner join symbols s on r.symbol_key = s.symbol_key
left join macro_rates m on r.date_day = m.date_day
