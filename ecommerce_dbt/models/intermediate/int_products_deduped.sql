WITH products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY _ingestion_timestamp DESC
        ) AS row_num
    FROM products
)

SELECT
    product_id,
    product_name,
    category,
    price,
    _source_file,
    _ingestion_timestamp,
    _pipeline_run_id
FROM ranked
WHERE row_num = 1