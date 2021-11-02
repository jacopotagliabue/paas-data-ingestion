{% macro select_pageviews(from_table) %}

    {{
        config(
            alias='pageviews',
            cluster_by=['TO_DATE(request_timestamp)']
        )
    }}

    SELECT *
    FROM {{ from_table }}
    WHERE
            hit_type = 'pageview'
        AND NULLIF(TRIM(req_body:ec::STRING), '') IS NOT NULL

{% endmacro %}
