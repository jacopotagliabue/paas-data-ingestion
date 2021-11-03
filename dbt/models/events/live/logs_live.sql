{{
    config(
        alias='logs'
    )
}}


SELECT * FROM {{ ref('logs_unprocessed') }}
WHERE request_timestamp > (SELECT $1 FROM {{ ref('last_materialization_process') }})

UNION

SELECT * FROM {{ ref('logs') }}
WHERE request_timestamp <= (SELECT $1 FROM {{ ref('last_materialization_process') }})
