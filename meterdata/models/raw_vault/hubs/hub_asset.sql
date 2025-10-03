{{
  config(
    materialized='incremental',
    table_type='iceberg',
    schema='raw_vault',
    unique_key='asset_hk',
    incremental_strategy='merge'
  )
}}

{# Hub: Asset - Contains unique asset identifiers #}

WITH source AS (
    SELECT
        asset_hk,
        asset_id,
        load_ts,
        record_source,
        ROW_NUMBER() OVER (PARTITION BY asset_hk ORDER BY load_ts) as rn
    FROM {{ ref('stg_meter_data') }}
    WHERE asset_hk IS NOT NULL
)
, deduplicated AS (
    SELECT
        asset_hk,
        asset_id,
        load_ts,
        record_source
    FROM source
    WHERE rn = 1
)

{% if is_incremental() %}
, existing_keys AS (
    SELECT DISTINCT asset_hk
    FROM {{ this }}
)
{% endif %}

SELECT
    asset_hk,
    asset_id,
    load_ts,
    record_source
FROM deduplicated
{% if is_incremental() %}
WHERE asset_hk NOT IN (SELECT asset_hk FROM existing_keys)
{% endif %}
