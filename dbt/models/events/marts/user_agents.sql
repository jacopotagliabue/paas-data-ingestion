{{
    select_user_agents(
        from_date=False,
        to_date=env_var('DBT_STAGE_0_MAX_DATE'),
        materialized='incremental'
    )
}}
