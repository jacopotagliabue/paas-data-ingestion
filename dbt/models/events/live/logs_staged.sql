{% set threshold = get_events_threshold(ref('logs')) -%}


{{-
    select_logs(
        from_date=threshold,
        to_date=env_var('DBT_STAGE_1_MAX_DATE'),
        materialized='view',
        alias='logs_staged'
    )
}}
