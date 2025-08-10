{{
  config(
    table_type='iceberg',
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    s3_data_naming='schema_table_unique',
    on_schema_change='append_new_columns',
    properties={
      "format": "'PARQUET'",
      "partitioning" : ['day(creation_time)'],

    }
)
}}

-- Silver layer: clean and type-safe readings with incremental processing
with src as (
    select
        id,
        trim(meter_id) as meter_id,
        cast(reading_value as decimal(15,3)) as reading_value,
        cast(interval_start as timestamp(6)) as interval_start,
        cast(interval_end as timestamp(6)) as interval_end,
        cast(creation_time as timestamp(6)) as creation_time,
        upper(cast(reading_type as varchar)) as reading_type,
        upper(cast(unit_of_measure as varchar)) as unit_of_measure,
        upper(cast(quality_code as varchar)) as quality_code,
        upper(cast(status as varchar)) as status,
        date(creation_time) as creation_date
    from {{ source('raw', 'readings') }}
    {% if is_incremental() %}
      -- Only process new records since the last run
      where creation_time > (select max(creation_time) from {{ this }})
    {% endif %}
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
    creation_date,
    reading_type,
    unit_of_measure,
    quality_code,
    status
from filtered
