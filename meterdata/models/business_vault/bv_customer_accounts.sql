{{
  config(
    materialized='view',
    schema='business_vault'
  )
}}

/*
  Business Vault View: Customer Accounts
  Denormalized view joining customer hub, account hub, and their satellites
  for easy consumption by downstream applications
*/

with customers as (
    select
        customer_hk,
        customer_id,
        load_ts as customer_load_ts
    from {{ ref('hub_customer') }}
),

customer_details_ranked as (
    select
        customer_hk,
        customer_id as customer_detail_id,
        load_ts as customer_detail_load_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_hk ORDER BY load_ts DESC) as rn
    from {{ ref('sat_customer_details') }}
),

customer_details as (
    select
        customer_hk,
        customer_detail_id,
        customer_detail_load_ts
    from customer_details_ranked
    where rn = 1
),

accounts as (
    select
        customer_account_hk,
        customer_account_id,
        load_ts as account_load_ts
    from {{ ref('hub_customer_account') }}
),

account_details_ranked as (
    select
        customer_account_hk,
        customer_account_id as account_detail_id,
        load_ts as account_detail_load_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_account_hk ORDER BY load_ts DESC) as rn
    from {{ ref('sat_customer_acc_details') }}
),

account_details as (
    select
        customer_account_hk,
        account_detail_id,
        account_detail_load_ts
    from account_details_ranked
    where rn = 1
),

link_customer_account as (
    select
        link_customer_account_hk,
        customer_hk,
        customer_account_hk,
        load_ts as link_load_ts
    from {{ ref('link_customer_account') }}
)

select
    c.customer_id as installation_id,
    a.customer_account_id as meter_id,
    c.customer_hk,
    a.customer_account_hk,
    l.link_customer_account_hk,
    c.customer_load_ts,
    a.account_load_ts,
    l.link_load_ts
from customers c
inner join link_customer_account l on c.customer_hk = l.customer_hk
inner join accounts a on l.customer_account_hk = a.customer_account_hk
left join customer_details cd on c.customer_hk = cd.customer_hk
left join account_details ad on a.customer_account_hk = ad.customer_account_hk
