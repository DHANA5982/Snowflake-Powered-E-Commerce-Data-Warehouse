SELECT
    order_item_id,
    quantity,
    unit_price
FROM {{ ref('fact_sales') }}
WHERE quantity <= 0
    OR unit_price < 0