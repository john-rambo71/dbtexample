{{
    config(
        tags=['basic', 'staging']
    )
}}

WITH

required_fields AS(

    SELECT

        *

    FROM {{ source('online', 'market') }}

),
datatype_conversion AS (
    SELECT 
    Ord_id,
    Prod_id as p_id,
    Ship_id,
    Cust_id,
    convert(float,Sales) AS Sales,
    convert(float,Discount) AS Discount,
    convert(int,Order_Quantity) AS Order_Quantity,
    cast(Profit as DECIMAL(18,2)) as Profit,
    convert(float,Shipping_Cost) AS Shipping_cost,
    Product_Base_Margin
    --try_cast(Product_Base_Margin as float) as Product_Base_Margin 

    FROM required_fields

)

SELECT * FROM datatype_conversion