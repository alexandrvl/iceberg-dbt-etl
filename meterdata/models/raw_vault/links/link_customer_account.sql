{{
  config(
    materialized='incremental',
    table_type='iceberg',
    schema='raw_vault',
    unique_key='link_customer_account_hk',
    incremental_strategy='merge'
  )
}}

{# Link: Customer-Account - Connects customers to their accounts #}

WITH source AS (
    SELECT
        link_customer_account_hk,
        customer_hk,
        customer_account_hk,
        load_ts,
        record_source,
        ROW_NUMBER() OVER (PARTITION BY link_customer_account_hk ORDER BY load_ts) as rn
    FROM {{ ref('stg_meter_data') }}
    WHERE link_customer_account_hk IS NOT NULL
      AND customer_hk IS NOT NULL
      AND customer_account_hk IS NOT NULL
)
, deduplicated AS (
    SELECT
        link_customer_account_hk,
        customer_hk,
        customer_account_hk,
        load_ts,
        record_source
    FROM source
    WHERE rn = 1
)

{% if is_incremental() %}
, existing_keys AS (
    SELECT DISTINCT link_customer_account_hk
    FROM {{ this }}
)
{% endif %}

SELECT
    link_customer_account_hk,
    customer_hk,
    customer_account_hk,
    load_ts,
    record_source
FROM deduplicated
{% if is_incremental() %}
WHERE link_customer_account_hk NOT IN (SELECT link_customer_account_hk FROM existing_keys)
{% endif %}
