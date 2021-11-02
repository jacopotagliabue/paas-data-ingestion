{% macro select_purchases(from_table) %}

    {{
        config(
            alias='purchases',
            cluster_by=['TO_DATE(request_timestamp)']
        )
    }}

    SELECT
        NULLIF(UPPER(TRIM(req_body:ti::STRING)), '') AS transaction_id
        , NULLIF(TRIM(req_body:ta::STRING), '') AS transaction_affiliation
        , NULLIF(TRIM(req_body:tr::STRING), '') AS transaction_revenue
        , NULLIF(TRIM(req_body:tr::STRING), '') AS transaction_tax
        , NULLIF(TRIM(req_body:ts::STRING), '') AS transaction_shipping
        , NULLIF(TRIM(req_body:tcc::STRING), '') AS transaction_coupon_code
        , *
    FROM {{ from_table }}
    WHERE
            product_action = 'purchase'
        AND NULLIF(TRIM(req_body:ti::STRING), '') IS NOT NULL
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY request_timestamp ASC
        ) = 1

{% endmacro %}
