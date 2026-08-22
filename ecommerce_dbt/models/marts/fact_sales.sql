WITH orders AS(
    SELECT *
    FROM {{ ref('int_orders_deduped') }}
),

order_items AS (
    SELECT *
    FROM {{ ref('int_order_items_deduped') }}

),

sales AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        o.customer_id,
        oi.product_id,

        o.order_date,
        o.status,

        oi.quantity,
        oi.unit_price,

        oi.quantity * oi.unit_price AS sales_amount

    FROM order_items oi
    INNER JOIN orders o
        ON oi.order_id = o.order_id
)

SELECT *
FROM sales
