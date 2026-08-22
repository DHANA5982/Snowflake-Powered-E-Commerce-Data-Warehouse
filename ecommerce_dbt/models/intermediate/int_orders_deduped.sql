WITH orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _ingestion_timestamp DESC
        ) AS row_num
    FROM orders
)

SELECT
    order_id,
    customer_id,
    order_date,
    status,
    _source_file,
    _ingestion_timestamp,
    _pipeline_run_id
FROM ranked
WHERE row_num = 1