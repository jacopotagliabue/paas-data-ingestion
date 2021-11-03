{% macro select_product_actions(from_table) %}

    {{
        config(
            alias='product_actions',
            cluster_by=['TO_DATE(request_timestamp)', 'product_action']
        )
    }}

    SELECT
          UPPER(p.value:id::STRING) AS product_id
        , UPPER(p.value:variant::STRING) AS product_variant
        , p.value:position::INT AS product_position
        , p.value:quantity::INT AS product_quantity
        , p.value:price::DECIMAL(10, 2) AS product_price
        , p.value:name::STRING AS product_name
        , p.value:brand::STRING AS product_brand
        , p.value:description::STRING AS product_description
        , p.value:raw::VARIANT AS product_raw_data
        , NULLIF(LOWER(TRIM(l.req_body:pal::STRING)), '') AS product_action_list
        , NULLIF(UPPER(TRIM(l.req_body:cu::STRING)), '') AS currency_code
        , NULLIF(TRIM(l.req_body:ti::STRING), '') AS transaction_id
        , l.*
    FROM
        {{ from_table }} AS l
        , LATERAL FLATTEN(input => {{ target.schema }}.udf_collect_ec_products_map(l.req_body::variant)) AS p
    WHERE l.product_action IS NOT NULL

{% endmacro %}
