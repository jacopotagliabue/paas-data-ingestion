{{
    config(
        alias='purchases'
    )
}}


SELECT * FROM {{ ref('purchases_unprocessed') }}

UNION ALL

SELECT * FROM {{ ref('purchases') }}
