{{
  config(
    materialized='incremental',
    table_type='iceberg',
    schema='raw_vault',
    unique_key=['asset_hk', 'load_ts'],
    incremental_strategy='merge',
    properties={
      "format": "'PARQUET'",
      "partitioning": ['day(load_ts)']
    }
  )
}}

{# Satellite: Asset Measurements - Time-series measurement data for assets #}

WITH source AS (
    SELECT
        asset_hk,
        asset_measurements_hashdiff,
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
, ranked_records AS (
    SELECT
        asset_hk,
        asset_measurements_hashdiff,
        ROW_NUMBER() OVER (PARTITION BY asset_hk ORDER BY load_ts DESC) as rn
    FROM {{ this }}
)
, latest_records AS (
    SELECT
        asset_hk,
        asset_measurements_hashdiff
    FROM ranked_records
    WHERE rn = 1
)
, records_to_insert AS (
    SELECT s.*
    FROM source s
    LEFT JOIN latest_records l
        ON s.asset_hk = l.asset_hk
       AND s.asset_measurements_hashdiff = l.asset_measurements_hashdiff
    WHERE l.asset_hk IS NULL  -- New or changed records
)
SELECT * FROM records_to_insert
{% else %}
SELECT * FROM source
{% endif %}
