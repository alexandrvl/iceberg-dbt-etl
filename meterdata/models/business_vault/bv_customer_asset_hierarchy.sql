{{
  config(
    materialized='view',
    schema='business_vault'
  )
}}

/*
  Business Vault View: Customer Asset Hierarchy
  Complete hierarchy from customer -> account -> asset
  Shows the full relationship chain
*/

with customers as (
    select
        customer_hk,
        customer_id as installation_id
    from {{ ref('hub_customer') }}
),

accounts as (
    select
        customer_account_hk,
        customer_account_id as meter_id
    from {{ ref('hub_customer_account') }}
),

assets as (
    select
        asset_hk,
        asset_id as asset_meter_id
    from {{ ref('hub_asset') }}
),

link_customer_account as (
    select
        customer_hk,
        customer_account_hk
    from {{ ref('link_customer_account') }}
),

link_account_asset as (
    select
        customer_account_hk,
        asset_hk
    from {{ ref('link_account_asset') }}
),

asset_details_ranked as (
    select
        asset_hk,
        asset_type,
        asset_installation_date as installation_date,
        asset_status,
        load_ts,
        ROW_NUMBER() OVER (PARTITION BY asset_hk ORDER BY load_ts DESC) as rn
    from {{ ref('sat_asset_details') }}
),

asset_details as (
    select
        asset_hk,
        asset_type,
        installation_date,
        asset_status
    from asset_details_ranked
    where rn = 1
)

select
    c.installation_id,
    acc.meter_id as account_meter_id,
    ast.asset_meter_id,
    ad.asset_type as meter_type,
    ad.installation_date,
    ad.asset_status as meter_status,
    c.customer_hk,
    acc.customer_account_hk,
    ast.asset_hk
from customers c
inner join link_customer_account lca on c.customer_hk = lca.customer_hk
inner join accounts acc on lca.customer_account_hk = acc.customer_account_hk
inner join link_account_asset laa on acc.customer_account_hk = laa.customer_account_hk
inner join assets ast on laa.asset_hk = ast.asset_hk
left join asset_details ad on ast.asset_hk = ad.asset_hk
