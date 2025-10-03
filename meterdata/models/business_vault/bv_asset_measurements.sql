{{
  config(
    materialized='view',
    schema='business_vault'
  )
}}

/*
  Business Vault View: Asset Measurements
  Complete view of all measurements with asset context
  Useful for analytics and reporting
*/

with assets as (
    select
        asset_hk,
        asset_id
    from {{ ref('hub_asset') }}
),

measurements as (
    select
        asset_hk,
        reading_id,
        reading_value,
        interval_start,
        interval_end,
        reading_type,
        unit_of_measure,
        quality_code,
        reading_status,
        load_ts
    from {{ ref('sat_asset_measurements') }}
),

asset_details_ranked as (
    select
        asset_hk,
        asset_type,
        asset_status,
        load_ts,
        ROW_NUMBER() OVER (PARTITION BY asset_hk ORDER BY load_ts DESC) as rn
    from {{ ref('sat_asset_details') }}
),

asset_details as (
    select
        asset_hk,
        asset_type,
        asset_status
    from asset_details_ranked
    where rn = 1
)

select
    a.asset_id as meter_id,
    ad.asset_type as meter_type,
    ad.asset_status as meter_status,
    m.reading_id,
    m.reading_value,
    m.interval_start,
    m.interval_end,
    m.reading_type,
    m.unit_of_measure,
    m.quality_code,
    m.reading_status,
    m.load_ts as measurement_load_ts,
    a.asset_hk
from measurements m
inner join assets a on m.asset_hk = a.asset_hk
left join asset_details ad on a.asset_hk = ad.asset_hk
