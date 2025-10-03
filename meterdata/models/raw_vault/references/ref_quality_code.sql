{{
  config(
    materialized='table',
    table_type='iceberg',
    schema='raw_vault'
  )
}}

-- Reference table for quality codes
select
    'GOOD' as quality_code,
    'Good Quality' as quality_name,
    'Reading passed all quality checks' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source
union all
select
    'SUSPECT' as quality_code,
    'Suspect Quality' as quality_name,
    'Reading flagged for review' as description,
    current_timestamp as load_ts,
    'SYSTEM' as record_source