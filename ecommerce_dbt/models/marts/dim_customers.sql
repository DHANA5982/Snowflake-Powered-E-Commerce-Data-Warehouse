WITH customers AS (
    SELECT *
    FROM {{ ref('int_customers_deduped') }}
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    country,
    created_at
FROM customers