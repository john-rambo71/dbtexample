{{
    config(
        tags=['mart']
    )
}}

WITH
stg_customer AS (
    SELECT
        *
    FROM {{ ref('stg_customer') }}
),
non_NUNAVUT AS (
    select 
        *
    from stg_customer
    where not Region ='NUNAVUT'
),
stg_market as (
    select *
    from {{ ref('stg_market') }}
),
stg_order as(
    select
        *
    FROM {{ ref('stg_order') }}
),
result as
(
    select
    stg_market.ord_id
    ,non_NUNAVUT.region
    ,SUM(Profit) as net_profit
    from stg_market
    right join non_NUNAVUT on stg_market.Cust_id = non_NUNAVUT.id
    left join stg_order on stg_market.ord_id = stg_order.out_id
    GROUP BY stg_market.Ord_id,non_NUNAVUT.region
)
select * from result