{{
    config(
        alias='events'
    )
}}


SELECT * FROM {{ ref('events_unprocessed') }}

UNION ALL

SELECT * FROM {{ ref('events') }}
