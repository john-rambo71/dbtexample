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
    select
    *
    from {{ref('stg_market')}}
),
stg_product as (
    select
    *
    from {{ref('stg_product')}}
),
joins as (
    select 
    stg_market.ord_id,
    stg_market.p_id as prod_id,
    stg_market.ship_id,
    stg_market.cust_id,
    stg_market.sales,
    stg_market.Discount,
    stg_market.Order_Quantity,
    stg_market.Profit,
    stg_market.Shipping_Cost,
    stg_market.Product_Base_Margin,
    non_NUNAVUT.region,
    stg_product.product_category,
    stg_product.product_sub_category 
    from stg_market
    right join non_NUNAVUT on non_NUNAVUT.id = stg_market.cust_id
    left join stg_product on stg_product.prod_id = stg_market.p_id
)
select * from joins