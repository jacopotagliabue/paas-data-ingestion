{{
    config(
        alias='pageviews'
    )
}}


SELECT * FROM {{ ref('pageviews_unprocessed') }}
WHERE request_timestamp > (SELECT $1 FROM {{ ref('last_materialization_process') }})

UNION

SELECT * FROM {{ ref('pageviews') }}
WHERE request_timestamp <= (SELECT $1 FROM {{ ref('last_materialization_process') }})
