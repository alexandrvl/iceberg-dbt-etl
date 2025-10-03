{{
  config(
    materialized='incremental',
    table_type='iceberg',
    schema='raw_vault',
    unique_key=['customer_hk', 'load_ts'],
    incremental_strategy='merge'
  )
}}

{# Satellite: Customer Details - Descriptive attributes for customers #}

WITH source AS (
    SELECT
        customer_hk,
        customer_hashdiff,
        customer_id,
        customer_name,
        customer_type,
        address,
        city,
        postal_code,
        country,
        registration_date,
        customer_status,
        load_ts,
        record_source
    FROM {{ ref('stg_meter_data') }}
    WHERE customer_hk IS NOT NULL
)

{% if is_incremental() %}
, ranked_records AS (
    SELECT
        customer_hk,
        customer_hashdiff,
        ROW_NUMBER() OVER (PARTITION BY customer_hk ORDER BY load_ts DESC) as rn
    FROM {{ this }}
)
, latest_records AS (
    SELECT
        customer_hk,
        customer_hashdiff
    FROM ranked_records
    WHERE rn = 1
)
, records_to_insert AS (
    SELECT s.*
    FROM source s
    LEFT JOIN latest_records l
        ON s.customer_hk = l.customer_hk
       AND s.customer_hashdiff = l.customer_hashdiff
    WHERE l.customer_hk IS NULL  -- New or changed records
)
SELECT * FROM records_to_insert
{% else %}
SELECT * FROM source
{% endif %}
