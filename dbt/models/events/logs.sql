SELECT * FROM {{ ref('logs_live') }}
WHERE request_timestamp > (SELECT $1 FROM {{ ref('last_event_materialized') }})

UNION

SELECT * FROM {{ ref('logs_materialized') }}
WHERE request_timestamp <= (SELECT $1 FROM {{ ref('last_event_materialized') }})
