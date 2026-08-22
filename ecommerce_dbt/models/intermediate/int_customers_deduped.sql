WITH customers AS (
    SELECT *
    FROM {{ ref('stg_customers') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY _ingestion_timestamp DESC
        ) AS row_num
    FROM customers
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    country,
    created_at,
    _source_file,
    _ingestion_timestamp,
    _pipeline_run_id

FROM ranked
WHERE row_num = 1