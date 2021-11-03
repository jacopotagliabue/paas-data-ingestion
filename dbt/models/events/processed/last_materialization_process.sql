SELECT
      MAX(REQUEST_TIMESTAMP)::timestamp_ntz AS max_request_timestamp
    , '{{ invocation_id }}' AS dbt_invocation_id
    , '{{ run_started_at.astimezone(modules.pytz.timezone("UTC")) }}'::timestamp_ntz AS dbt_run_started_at
    , '{{ target.profile_name }}' AS dbt_target_profile_name
    , '{{ target.name }}' AS dbt_target_name
    , '{{ target.threads }}' AS dbt_threads
    , '{{ dbt_version }}' AS dbt_version
    , SYSDATE() AS run_timestamp
FROM {{ ref('logs') }}
