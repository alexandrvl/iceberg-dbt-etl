{{ config(materialized='table') }}

-- Silver layer: clean and type-safe readings
with src as (
    select
        id,
        trim(meter_id) as meter_id,
        cast(reading_value as decimal(15,3)) as reading_value,
        cast(interval_start as timestamp) as interval_start,
        cast(interval_end as timestamp) as interval_end,
        cast(creation_time as timestamp) as creation_time,
        upper(cast(reading_type as varchar)) as reading_type,
        upper(cast(unit_of_measure as varchar)) as unit_of_measure,
        upper(cast(quality_code as varchar)) as quality_code,
        upper(cast(status as varchar)) as status
    from {{ source('raw', 'readings') }}
),

-- basic quality filters
filtered as (
    select *
    from src
    where meter_id is not null
      and reading_value is not null
      and interval_start is not null
)

select
    id,
    meter_id,
    reading_value,
    interval_start,
    interval_end,
    creation_time,
    reading_type,
    unit_of_measure,
    quality_code,
    status
from filtered
