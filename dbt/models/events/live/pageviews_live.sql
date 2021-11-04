{{
    config(
        alias='pageviews'
    )
}}


SELECT * FROM {{ ref('pageviews_unprocessed') }}

UNION ALL

SELECT * FROM {{ ref('pageviews') }}
