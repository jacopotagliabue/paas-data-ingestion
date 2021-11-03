{{
    select_logs(
        from_date=env_var('DBT_STAGE_0_MAX_DATE'),
        to_date=env_var('DBT_STAGE_1_MAX_DATE')
    )
}}
