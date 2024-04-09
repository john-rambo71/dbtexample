{{
    config(
        tags=['basic', 'staging']
    )
}}

WITH

required_fields AS(

    SELECT

        *

    FROM {{ source('online', 'order') }}

),
datatype_conversion AS (

    SELECT 

        cast(order_ID as int) as order_id
        ,convert(date,order_date,105) as order_date
        ,order_priority
        ,ord_id as out_id

    FROM required_fields

)

SELECT * FROM datatype_conversion