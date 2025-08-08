{{ config(materialized='table') }}

-- Gold layer: daily consumption per meter based on cleaned readings
with s as (
    select * from {{ ref('readings__silver') }}
),

agg as (
    select
        meter_id,
        cast(date(interval_start) as date) as reading_date,
        reading_type,
        unit_of_measure,
        sum(reading_value) as total_value,
        count(*) as reading_count,
        min(interval_start) as first_interval_start,
        max(interval_end) as last_interval_end,
        max(creation_time) as last_creation_time
    from s
    group by 1,2,3,4
)

select * from agg
