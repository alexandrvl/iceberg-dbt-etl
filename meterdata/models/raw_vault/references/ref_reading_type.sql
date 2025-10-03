{{
  config(
    materialized='table',
    table_type='iceberg',
    schema='raw_vault'
  )
}}

-- Reference table for reading types
select
    'CONSUMPTION' as reading_type_code,
    'Consumption Reading' as reading_type_name,
    'Interval consumption measurement' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source
union all
select
    'DEMAND' as reading_type_code,
    'Demand Reading' as reading_type_name,
    'Peak demand measurement' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source