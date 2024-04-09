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
stg_order as (
    select *
    from {{ ref('stg_order') }}
),
all_joins as (
    select 
    *
    from stg_market
    right join non_NUNAVUT on stg_market.Cust_id = non_NUNAVUT.id
    left join stg_order on stg_market.ord_id = stg_order.out_id
),
all_details as(
    select 
    id
    ,Name
    ,Region
    ,min(order_date) as first_order_date
    ,max(order_date) as recent_order_date
    ,count(cust_id) as order_count
    ,sum(sales) as total_spent
    from all_joins
	group by id,name,region
)
SELECT * FROM all_details;