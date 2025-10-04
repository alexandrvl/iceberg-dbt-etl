{{
  config(
    materialized='view'
  )
}}

{#-
  Staging layer for Data Vault 2.0

  This model joins all source tables and prepares them for the raw vault by:
  1. Cleaning and standardizing data
  2. Generating hash keys for hubs
  3. Generating link hash keys
  4. Generating hash diffs for satellites

  Data Vault Mapping:
  - Hub Customer: customers.installation_id
  - Hub Customer Account: customer_accounts.account_id
  - Hub Asset: assets.asset_id
  - Link Customer-Account: customers + customer_accounts
  - Link Account-Asset: customer_accounts + assets
  - Sat Customer Details: customer attributes
  - Sat Account Details: account attributes
  - Sat Asset Details: asset attributes
  - Sat Asset Measurements: readings
-#}

with customers as (
    select * from {{ source('raw', 'customers') }}
),

customer_accounts as (
    select * from {{ source('raw', 'customer_accounts') }}
),

assets as (
    select * from {{ source('raw', 'assets') }}
),

readings as (
    select * from {{ source('raw', 'readings') }}
),

-- Join all entities together
enriched as (
    select
        -- From customers
        c.installation_id,
        c.customer_name,
        c.customer_type,
        c.address,
        c.city,
        c.postal_code,
        c.country,
        c.registration_date,
        c.last_modified as customer_last_modified,
        c.status as customer_status,

        -- From customer_accounts
        ca.account_id,
        ca.account_number,
        ca.account_type,
        ca.billing_cycle,
        ca.account_opened_date,
        ca.account_closed_date,
        ca.last_modified as account_last_modified,
        ca.status as account_status,

        -- From assets
        a.asset_id,
        a.asset_serial_number,
        a.asset_type,
        a.manufacturer,
        a.model,
        a.installation_date as asset_installation_date,
        a.last_calibration_date,
        a.next_calibration_date,
        a.last_modified as asset_last_modified,
        a.status as asset_status,

        -- From readings
        r.id as reading_id,
        r.reading_value,
        r.interval_start,
        r.interval_end,
        r.creation_time,
        r.reading_type,
        r.unit_of_measure,
        r.quality_code,
        r.status as reading_status,
        r.source_system

    from readings r
    inner join assets a on r.asset_id = a.asset_id
    inner join customer_accounts ca on a.account_id = ca.account_id
    inner join customers c on ca.installation_id = c.installation_id
)

select
    -- Business Keys
    installation_id as customer_id,
    account_id as customer_account_id,
    asset_id,

    -- Natural Keys for Hubs
    installation_id as customer_hk,
    account_id as customer_account_hk,
    asset_id as asset_hk,

    -- Hash Keys for Links
    {{ hash_key(['installation_id', 'account_id']) }} as link_customer_account_hk,
    {{ hash_key(['account_id', 'asset_id']) }} as link_account_asset_hk,

    -- Hash Diffs for Satellites
    {{ hash_diff(['customer_name', 'customer_type', 'address', 'city', 'postal_code', 'country', 'customer_status']) }} as customer_hashdiff,
    {{ hash_diff(['account_number', 'account_type', 'billing_cycle', 'account_status']) }} as customer_account_hashdiff,
    {{ hash_diff(['asset_serial_number', 'asset_type', 'manufacturer', 'model', 'asset_installation_date', 'asset_status']) }} as asset_details_hashdiff,

    -- Customer Details (Satellite Payload)
    customer_name,
    customer_type,
    address,
    city,
    postal_code,
    country,
    registration_date,
    customer_status,

    -- Account Details (Satellite Payload)
    account_number,
    account_type,
    billing_cycle,
    account_opened_date,
    account_closed_date,
    account_status,

    -- Asset Details (Satellite Payload)
    asset_serial_number,
    asset_type,
    manufacturer,
    model,
    asset_installation_date,
    last_calibration_date,
    next_calibration_date,
    asset_status,

    -- Measurement Details (Satellite Payload)
    reading_id,
    reading_value,
    interval_start,
    interval_end,
    reading_type,
    unit_of_measure,
    quality_code,
    reading_status,

    -- Metadata
    creation_time as load_ts,
    coalesce(source_system, 'METER_DATA_SYSTEM') as record_source

from enriched
where installation_id is not null
  and account_id is not null
  and asset_id is not null
