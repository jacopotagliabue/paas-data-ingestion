{{
    config(
        alias='purchases'
    )
}}


SELECT * FROM {{ ref('purchases_unprocessed') }}
WHERE request_timestamp > (SELECT $1 FROM {{ ref('last_materialization_process') }})

UNION

SELECT * FROM {{ ref('purchases') }}
WHERE request_timestamp <= (SELECT $1 FROM {{ ref('last_materialization_process') }})
