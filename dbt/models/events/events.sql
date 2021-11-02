SELECT * FROM {{ ref('events_live') }}
WHERE request_timestamp > (SELECT $1 FROM {{ ref('last_event_materialized') }})

UNION

SELECT * FROM {{ ref('events_materialized') }}
WHERE request_timestamp <= (SELECT $1 FROM {{ ref('last_event_materialized') }})
