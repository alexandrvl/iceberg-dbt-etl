{{
  config(
    materialized='incremental',
    table_type='iceberg',
    schema='raw_vault',
    unique_key='customer_hk',
    incremental_strategy='merge'
  )
}}

{# Hub: Customer - Contains unique customer identifiers #}

WITH source AS (
    SELECT
        customer_hk,
        customer_id,
        load_ts,
        record_source,
        ROW_NUMBER() OVER (PARTITION BY customer_hk ORDER BY load_ts) as rn
    FROM {{ ref('stg_meter_data') }}
    WHERE customer_hk IS NOT NULL
)
, deduplicated AS (
    SELECT
        customer_hk,
        customer_id,
        load_ts,
        record_source
    FROM source
    WHERE rn = 1
)

{% if is_incremental() %}
, existing_keys AS (
    SELECT DISTINCT customer_hk
    FROM {{ this }}
)
{% endif %}

SELECT
    customer_hk,
    customer_id,
    load_ts,
    record_source
FROM deduplicated
{% if is_incremental() %}
WHERE customer_hk NOT IN (SELECT customer_hk FROM existing_keys)
{% endif %}
