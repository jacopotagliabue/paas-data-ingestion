{% macro select_stats_overview(alias, date_agg, cluster_by='') %}
    {{
        config(
            alias=alias,
            cluster_by=cluster_by
        )
    }}

    WITH
        overall AS (
            SELECT
                  {{ date_agg }} AS period
                , COUNT(DISTINCT session_id) AS active_sessions
                , COUNT(DISTINCT user_id) AS active_users
            FROM {{ ref('logs_materialized') }}
            GROUP BY period
        )
        , pageviews AS (
            SELECT
                  {{ date_agg }} AS period
                , COUNT(*) AS _count
            FROM {{ ref('pageviews_materialized')}}
            GROUP BY period
        )
        , pa AS (
            SELECT
                  {{ date_agg }} AS period
                , product_action
                , COUNT(*) AS _count
                , COUNT(DISTINCT session_id, product_id) AS count_distinct_session_products
                , COUNT(DISTINCT product_id) AS count_distinct_product_ids
                , SUM(product_quantity) AS sum_product_quantity
            FROM {{ ref('product_actions_materialized')}}
            GROUP BY period, product_action
        )
        , purchases AS (
            SELECT
                  {{ date_agg }} AS period
                , COUNT(DISTINCT transaction_id) AS _count
                , SUM(transaction_revenue) AS sum_revenue
                , SUM(transaction_tax) AS sum_tax
                , SUM(transaction_shipping) AS sum_shipping
            FROM {{ ref('purchases_materialized')}}
            GROUP BY period
        )
        , new_sessions AS (
            SELECT
                  period
                , COUNT(DISTINCT session_id) AS count_distinct_session_ids
            FROM (
                SELECT DISTINCT
                      FIRST_VALUE({{ date_agg }}) OVER (
                        PARTITION BY TO_DATE(request_timestamp), session_id
                        ORDER BY request_timestamp ASC
                    ) AS period
                    , session_id
                FROM {{ ref('logs_materialized') }}
            )
            GROUP BY period
        )
        , new_users AS (
            SELECT
                  period
                , COUNT(DISTINCT user_id) AS count_distinct_user_ids
            FROM (
                SELECT DISTINCT
                      FIRST_VALUE({{ date_agg }}) OVER (
                        PARTITION BY TO_DATE(request_timestamp), user_id
                        ORDER BY request_timestamp ASC
                    ) AS period
                    , user_id
                FROM {{ ref('logs_materialized') }}
            )
            GROUP BY period
        )


    SELECT
          o.period

        , IFNULL(o.active_sessions, 0) AS active_sessions
        , IFNULL(new_sessions.count_distinct_session_ids, 0) AS new_sessions

        , IFNULL(o.active_users, 0) AS active_users
        , IFNULL(new_users.count_distinct_user_ids, 0) AS new_users

        , IFNULL(pageviews._count, 0) AS pageviews

        , IFNULL(pa_click._count, 0) AS sum_of_pa_click
        , IFNULL(pa_click.count_distinct_session_products, 0) AS pa_clicks

        , IFNULL(pa_detail._count, 0) AS sum_of_pa_detail
        , IFNULL(pa_detail.count_distinct_session_products, 0) AS pa_details

        , IFNULL(pa_add._count, 0) AS sum_of_pa_add
        , IFNULL(pa_add.count_distinct_session_products, 0) AS pa_adds

        , IFNULL(pa_remove._count, 0) AS sum_of_pa_remove
        , IFNULL(pa_remove.count_distinct_session_products, 0) AS pa_removes

        , IFNULL(purchases._count, 0) AS transactions
        , IFNULL(purchases.sum_revenue, 0.0) AS transaction_revenue
        , IFNULL(purchases.sum_tax, 0.0) AS transaction_tax
        , IFNULL(purchases.sum_shipping, 0.0) AS transaction_shipping
        , DIV0(transaction_revenue, transactions) AS avg_order_value

        , IFNULL(pa_purchase.sum_product_quantity, 0) AS purchased_items
        , IFNULL(pa_purchase.count_distinct_product_ids, 0) AS purchased_products

    FROM overall AS o

    LEFT JOIN new_sessions ON
            new_sessions.period = o.period
    LEFT JOIN new_users ON
            new_users.period = o.period

    LEFT JOIN pageviews ON
            pageviews.period = o.period

    LEFT JOIN pa AS pa_click ON
            pa_click.period = o.period
        AND pa_click.product_action = 'click'
    LEFT JOIN pa AS pa_add ON
            pa_add.period = o.period
        AND pa_add.product_action = 'add'
    LEFT JOIN pa AS pa_remove ON
            pa_remove.period = o.period
        AND pa_remove.product_action = 'remove'
    LEFT JOIN pa AS pa_detail ON
            pa_detail.period = o.period
        AND pa_detail.product_action = 'detail'
    LEFT JOIN pa AS pa_purchase ON
            pa_purchase.period = o.period
        AND pa_purchase.product_action = 'purchase'

    LEFT JOIN purchases ON
            purchases.period = o.period

    ORDER BY period DESC

{% endmacro %}
