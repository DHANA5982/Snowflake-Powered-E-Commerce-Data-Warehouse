WITH order_item AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY _ingestion_timestamp DESC
        ) AS row_num
    FROM order_item
)

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    _source_file,
    _ingestion_timestamp,
    _pipeline_run_id
FROM ranked
WHERE row_num = 1