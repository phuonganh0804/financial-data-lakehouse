-- Unified daily price rows across crypto and equities. The single place the
-- two price sources are combined; reused by fct_daily_prices and dim_symbol.
with crypto as (
    select
        date_day,
        symbol,
        exchange,
        asset_class,
        currency,
        open,
        high,
        low,
        close,
        volume
    from {{ ref('stg_binance_klines') }}
),

equity as (
    select
        date_day,
        symbol,
        exchange,
        asset_class,
        currency,
        open,
        high,
        low,
        close,
        volume
    from {{ ref('stg_equity_prices') }}
)

select * from crypto
union all
select * from equity
