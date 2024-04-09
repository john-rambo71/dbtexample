{{
    config(
        tags=['basic', 'staging']
    )
}}

WITH

required_fields AS(

    SELECT

        *

    FROM {{ source('online', 'customer') }}

),
datatype_conversion AS (

    SELECT 

        Customer_Name as name
        ,Province
        ,Region
        ,Customer_Segment as Segment
        ,Cust_id as id

    FROM required_fields

)

SELECT * FROM datatype_conversion