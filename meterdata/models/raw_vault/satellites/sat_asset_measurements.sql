{{
  config(
    materialized='incremental',
    table_type='iceberg',
    schema='raw_vault',
    unique_key=['asset_hk', 'interval_start', 'interval_end', 'reading_type', 'load_ts'],
    incremental_strategy='merge',
    properties={
      "format": "'PARQUET'",
      "partitioning": ['day(load_ts)']
    }
  )
}}

{# Satellite: Asset Measurements - Time-series measurement data for assets #}
{# Uniqueness: asset_hk + interval_start + interval_end + reading_type + load_ts #}

WITH source AS (
    SELECT
        asset_hk,
        reading_id,
        reading_value,
        interval_start,
        interval_end,
        reading_type,
        unit_of_measure,
        quality_code,
        reading_status,
        load_ts,
        record_source
    FROM {{ ref('stg_meter_data') }}
    WHERE asset_hk IS NOT NULL
)

{% if is_incremental() %}
, existing_measurements AS (
    SELECT
        asset_hk,
        interval_start,
        interval_end,
        reading_type,
        load_ts
    FROM {{ this }}
)
, records_to_insert AS (
    SELECT s.*
    FROM source s
    LEFT JOIN existing_measurements e
        ON s.asset_hk = e.asset_hk
       AND s.interval_start = e.interval_start
       AND s.interval_end = e.interval_end
       AND s.reading_type = e.reading_type
       AND s.load_ts = e.load_ts
    WHERE e.asset_hk IS NULL  -- Only new measurements
)
SELECT * FROM records_to_insert
{% else %}
SELECT * FROM source
{% endif %}
