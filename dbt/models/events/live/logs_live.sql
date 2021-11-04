{{
    config(
        alias='logs'
    )
}}


SELECT * FROM {{ ref('logs_unprocessed') }}

UNION ALL

SELECT * FROM {{ ref('logs') }}
