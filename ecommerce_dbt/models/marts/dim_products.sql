WITH products AS (
    SELECT *
    FROM {{ ref('int_products_deduped') }}
)

SELECT
    product_id,
    product_name,
    category,
    price
FROM products