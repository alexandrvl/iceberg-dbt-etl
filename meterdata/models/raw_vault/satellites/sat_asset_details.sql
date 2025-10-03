{{
  config(
    materialized='incremental',
    table_type='iceberg',
    schema='raw_vault',
    unique_key=['asset_hk', 'load_ts'],
    incremental_strategy='merge'
  )
}}

{# Satellite: Asset Details - Descriptive attributes for assets/meters #}

WITH source AS (
    SELECT
        asset_hk,
        asset_details_hashdiff,
        asset_id,
        asset_serial_number,
        asset_type,
        manufacturer,
        model,
        asset_installation_date,
        last_calibration_date,
        next_calibration_date,
        asset_status,
        load_ts,
        record_source
    FROM {{ ref('stg_meter_data') }}
    WHERE asset_hk IS NOT NULL
)

{% if is_incremental() %}
, ranked_records AS (
    SELECT
        asset_hk,
        asset_details_hashdiff,
        ROW_NUMBER() OVER (PARTITION BY asset_hk ORDER BY load_ts DESC) as rn
    FROM {{ this }}
)
, latest_records AS (
    SELECT
        asset_hk,
        asset_details_hashdiff
    FROM ranked_records
    WHERE rn = 1
)
, records_to_insert AS (
    SELECT s.*
    FROM source s
    LEFT JOIN latest_records l
        ON s.asset_hk = l.asset_hk
       AND s.asset_details_hashdiff = l.asset_details_hashdiff
    WHERE l.asset_hk IS NULL  -- New or changed records
)
SELECT * FROM records_to_insert
{% else %}
SELECT * FROM source
{% endif %}
