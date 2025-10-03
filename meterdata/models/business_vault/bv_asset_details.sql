{{
  config(
    materialized='view',
    schema='business_vault'
  )
}}

/*
  Business Vault View: Asset Details
  Complete view of assets (meters) with all their descriptive attributes
*/

with assets as (
    select
        asset_hk,
        asset_id,
        load_ts as asset_load_ts
    from {{ ref('hub_asset') }}
),

asset_details_ranked as (
    select
        asset_hk,
        asset_id,
        asset_type,
        asset_installation_date as installation_date,
        asset_status,
        load_ts as detail_load_ts,
        ROW_NUMBER() OVER (PARTITION BY asset_hk ORDER BY load_ts DESC) as rn
    from {{ ref('sat_asset_details') }}
),

asset_details as (
    select
        asset_hk,
        asset_id,
        asset_type,
        installation_date,
        asset_status,
        detail_load_ts
    from asset_details_ranked
    where rn = 1
)

select
    a.asset_id as meter_id,
    a.asset_hk,
    ad.asset_type as meter_type,
    ad.installation_date,
    ad.asset_status as meter_status,
    a.asset_load_ts,
    ad.detail_load_ts
from assets a
left join asset_details ad on a.asset_hk = ad.asset_hk
