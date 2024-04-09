{{
    config(
        tags=['basic', 'staging']
    )
}}

WITH

required_fields AS(

    SELECT

        *

    FROM {{ source('online', 'product') }}

)

SELECT * FROM required_fields