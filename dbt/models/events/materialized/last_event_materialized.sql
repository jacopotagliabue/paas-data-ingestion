{{
    config(
        alias='last_event'
    )
}}


SELECT MAX(REQUEST_TIMESTAMP) AS max_request_timestamp
FROM {{ ref('logs_materialized') }}
