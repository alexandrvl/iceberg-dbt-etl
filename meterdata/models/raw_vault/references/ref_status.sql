{{
  config(
    materialized='table',
    table_type='iceberg',
    schema='raw_vault'
  )
}}

-- Reference table for status codes
select
    'ACTIVE' as status_code,
    'Active' as status_name,
    'Entity is active and operational' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source
union all
select
    'INACTIVE' as status_code,
    'Inactive' as status_name,
    'Entity is inactive' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source
union all
select
    'VALID' as status_code,
    'Valid' as status_name,
    'Reading is valid' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source
union all
select
    'ESTIMATED' as status_code,
    'Estimated' as status_name,
    'Reading is estimated' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source