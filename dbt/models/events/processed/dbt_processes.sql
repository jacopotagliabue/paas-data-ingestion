{{
    config(
        materialized='incremental'
    )
}}


SELECT
      '{{ invocation_id }}' AS invocation_id
    , '{{ run_started_at.astimezone(modules.pytz.timezone("UTC")) }}'::timestamp_ntz AS run_started_at
    , '{{ target.profile_name }}' AS target_profile_name
    , '{{ target.name }}' AS target_name
    , '{{ target.threads }}' AS threads
    , '{{ dbt_version }}' AS version
    , MAX(REQUEST_TIMESTAMP)::timestamp_ntz AS last_processed_request_timestamp
FROM {{ ref('logs') }}
