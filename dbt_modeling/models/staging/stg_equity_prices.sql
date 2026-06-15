with source as (
    select * from {{ source('silver', 'equity_prices') }}
)

select
    "date"   as date_day,
    symbol,
    'equity' as asset_class,
    open,
    high,
    low,
    close,
    volume,
    datetime,
    currency,
    exchange,
    "source" as data_source
from source
