{{
    config(
        alias='product_actions'
    )
}}


SELECT * FROM {{ ref('product_actions_unprocessed') }}

UNION ALL

SELECT * FROM {{ ref('product_actions') }}
