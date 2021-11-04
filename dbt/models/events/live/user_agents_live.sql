{{
    config(
        alias='user_agents'
    )
}}


SELECT * FROM {{ ref('user_agents_unprocessed' )}}

UNION ALL

SELECT * FROM {{ ref('user_agents' )}}
