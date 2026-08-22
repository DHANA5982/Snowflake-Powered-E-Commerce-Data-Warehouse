WITH source AS (
    SELECT *
    FROM {{ source('raw', 'raw_orders')}}
),

renamed AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        Lower(TRIM(status)) AS status,
        _source_file,
        _ingestion_timestamp,
        _pipeline_run_id

    FROM source
)

SELECT *
FROM renamed