{{
    config(
        cluster_by=['date']
    )
}}

WITH
      pa AS (
        SELECT
              TO_DATE(request_timestamp) AS date
            , product_action
            , product_id
            , COUNT(*) AS _count
            , COUNT(DISTINCT session_id) AS _count_distinct_sessions
            , COUNT(DISTINCT transaction_id) AS _count_distinct_transactions
            , SUM(product_quantity) AS _sum_quantity
            , SUM(product_quantity * product_price) AS _sum_revenue
            , MIN(product_price) AS _min_price
            , MAX(product_price) AS _max_price
        FROM {{ ref('product_actions')}}
        GROUP BY TO_DATE(request_timestamp), product_action, product_id
    )
    , overview AS (
        SELECT
              TO_DATE(request_timestamp) AS date
            , product_id
            , MIN(product_price) AS _min_price
            , MAX(product_price) AS _max_price
        FROM {{ ref('product_actions')}}
        GROUP BY TO_DATE(request_timestamp), product_id
    )

SELECT
      overview.date AS date
    , overview.product_id AS product_id
    , overview._min_price AS min_price
    , overview._max_price AS max_price

    , IFNULL(pa_click._count, 0) AS sum_of_pa_click
    , IFNULL(pa_click._count_distinct_sessions, 0) AS pa_clicks

    , IFNULL(pa_detail._count, 0) AS sum_of_pa_detail
    , IFNULL(pa_detail._count_distinct_sessions, 0) AS pa_details

    , IFNULL(pa_add._count, 0) AS sum_of_pa_add
    , IFNULL(pa_add._count_distinct_sessions, 0) AS pa_adds

    , IFNULL(pa_remove._count, 0) AS sum_of_pa_remove
    , IFNULL(pa_remove._count_distinct_sessions, 0) AS pa_removes

    , IFNULL(pa_purchase._count_distinct_transactions, 0) AS transactions
    , IFNULL(pa_purchase._sum_revenue, 0.0) AS transaction_revenue

    , IFNULL(pa_purchase._sum_quantity, 0) AS purchased_items
    , IFNULL(pa_purchase._count, 0) AS purchased_products
    , pa_purchase._min_price AS min_purchased_price
    , pa_purchase._max_price AS max_purchased_price

FROM overview

LEFT JOIN pa AS pa_click ON
        pa_click.date = overview.date
    AND pa_click.product_action = 'click'
    AND pa_click.product_id = overview.product_id
LEFT JOIN pa AS pa_add ON
        pa_add.date = overview.date
    AND pa_add.product_action = 'add'
    AND pa_add.product_id = overview.product_id
LEFT JOIN pa AS pa_remove ON
        pa_remove.date = overview.date
    AND pa_remove.product_action = 'remove'
    AND pa_remove.product_id = overview.product_id
LEFT JOIN pa AS pa_detail ON
        pa_detail.date = overview.date
    AND pa_detail.product_action = 'detail'
    AND pa_detail.product_id = overview.product_id
LEFT JOIN pa AS pa_purchase ON
        pa_purchase.date = overview.date
    AND pa_purchase.product_action = 'purchase'
    AND pa_purchase.product_id = overview.product_id
