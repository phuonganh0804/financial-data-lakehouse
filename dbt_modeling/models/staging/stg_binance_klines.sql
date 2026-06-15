with source as (
    select * from {{ source('silver', 'binance_klines') }}
)

select
    "date"                as date_day,
    symbol,
    'crypto'              as asset_class,
    'BINANCE'             as exchange,
    cast(null as varchar) as currency,
    open,
    high,
    low,
    close,
    volume,
    open_time,
    "source"              as data_source
from source
