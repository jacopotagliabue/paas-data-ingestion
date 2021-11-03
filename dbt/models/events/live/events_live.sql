{{
    config(
        alias='events'
    )
}}


SELECT * FROM {{ ref('events_unprocessed') }}
WHERE request_timestamp > (SELECT $1 FROM {{ ref('last_materialization_process') }})

UNION

SELECT * FROM {{ ref('events') }}
WHERE request_timestamp <= (SELECT $1 FROM {{ ref('last_materialization_process') }})
