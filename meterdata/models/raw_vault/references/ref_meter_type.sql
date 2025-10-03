{{
  config(
    materialized='table',
    table_type='iceberg',
    schema='raw_vault'
  )
}}

-- Reference table for meter types
select
    'ELECTRIC' as meter_type_code,
    'Electric Meter' as meter_type_name,
    'Measures electrical consumption' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source
union all
select
    'GAS' as meter_type_code,
    'Gas Meter' as meter_type_name,
    'Measures gas consumption' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source
union all
select
    'WATER' as meter_type_code,
    'Water Meter' as meter_type_name,
    'Measures water consumption' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source