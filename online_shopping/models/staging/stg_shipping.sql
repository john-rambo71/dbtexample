{{
    config(
        tags=['basic', 'staging']
    )
}}

WITH

required_fields AS(

    SELECT

        *

    FROM {{ source('online', 'shipping') }}

),
datatype_conversion AS (

    SELECT 

        cast(order_id as int) as order_id
        ,ship_mode
        ,convert(date,ship_date,105) as ship_date
        ,ship_id

    FROM required_fields

)

SELECT * FROM datatype_conversion