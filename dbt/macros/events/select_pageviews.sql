{% macro select_pageviews(from_table) %}

    {{
        config(
            alias='pageviews',
            cluster_by=['TO_DATE(request_timestamp)']
        )
    }}

    SELECT *
    FROM {{ from_table }}
    WHERE hit_type = 'pageview'

{% endmacro %}
