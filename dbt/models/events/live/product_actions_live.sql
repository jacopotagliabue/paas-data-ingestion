{{
    config(
        alias='product_actions'
    )
}}


SELECT * FROM {{ ref('product_actions_unprocessed') }}
WHERE request_timestamp > (SELECT $1 FROM {{ ref('last_materialization_process') }})

UNION

SELECT * FROM {{ ref('product_actions') }}
WHERE request_timestamp <= (SELECT $1 FROM {{ ref('last_materialization_process') }})
