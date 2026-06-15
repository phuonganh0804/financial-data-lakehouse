with source as (
    select * from {{ source('silver', 'fred_macro') }}
)

select
    "date"      as date_day,
    series_id,
    series_name,
    frequency,
    unit,
    value
from source
