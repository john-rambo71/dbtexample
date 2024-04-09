{{
    config(
        tags=['mart']
    )
}}

with 
stg_customer as (
    select *
    from {{ ref('stg_customer') }}
),

stg_order as (
    select *
    from {{ ref('stg_order') }}
),
stg_shipping as (
    select *
    from {{ ref('stg_shipping') }}
),
NUNAVUT as (
  select * 
  from stg_customer 
  where province != 'NUNAVUT'
), 
joins as (
  select 
    stg_shipping.order_id,
    stg_shipping.ship_id,
    stg_shipping.ship_date,
    stg_order.order_date,
    stg_order.out_id,
	stg_order.order_priority
  from 
    stg_order 
    inner join stg_shipping on stg_order.order_id = stg_shipping.Order_ID
),
Different_date as (
  select 
    *,
    DATEDIFF(day, order_date,ship_date) as time_delayed 
  from joins
),
 
result as 
(
    select * ,
    case
    when order_priority = 'HIGH' and (time_delayed  > 3) then 'true'
    else 'false'
    end as support
    from Different_date
)
 
select * from result;