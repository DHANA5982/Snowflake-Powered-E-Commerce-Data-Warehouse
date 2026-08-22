SELECT
    order_item_id,
    quantity,
    unit_price,
    sales_amount
FROM {{ ref('fact_sales') }}
WHERE sales_amount != quantity * unit_price