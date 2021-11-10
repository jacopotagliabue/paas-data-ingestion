{{
    config(
        alias='user_agents',
        materialized='view'
    )
-}}


{%- set threshold = get_events_threshold(ref('logs')) -%}


SELECT * FROM (
    {{
        select_user_agents(
            from_date=threshold,
            to_date=env_var('DBT_STAGE_1_MAX_DATE'),
        )
    }}
)

UNION ALL

SELECT *
FROM {{ ref('user_agents' )}}
