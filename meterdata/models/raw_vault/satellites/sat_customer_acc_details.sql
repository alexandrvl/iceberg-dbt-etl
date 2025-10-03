{{
  config(
    materialized='incremental',
    table_type='iceberg',
    schema='raw_vault',
    unique_key=['customer_account_hk', 'load_ts'],
    incremental_strategy='merge'
  )
}}

{# Satellite: Customer Account Details - Descriptive attributes for customer accounts #}

WITH source AS (
    SELECT
        customer_account_hk,
        customer_account_hashdiff,
        customer_account_id,
        account_number,
        account_type,
        billing_cycle,
        account_opened_date,
        account_closed_date,
        account_status,
        load_ts,
        record_source
    FROM {{ ref('stg_meter_data') }}
    WHERE customer_account_hk IS NOT NULL
)

{% if is_incremental() %}
, ranked_records AS (
    SELECT
        customer_account_hk,
        customer_account_hashdiff,
        ROW_NUMBER() OVER (PARTITION BY customer_account_hk ORDER BY load_ts DESC) as rn
    FROM {{ this }}
)
, latest_records AS (
    SELECT
        customer_account_hk,
        customer_account_hashdiff
    FROM ranked_records
    WHERE rn = 1
)
, records_to_insert AS (
    SELECT s.*
    FROM source s
    LEFT JOIN latest_records l
        ON s.customer_account_hk = l.customer_account_hk
       AND s.customer_account_hashdiff = l.customer_account_hashdiff
    WHERE l.customer_account_hk IS NULL  -- New or changed records
)
SELECT * FROM records_to_insert
{% else %}
SELECT * FROM source
{% endif %}
