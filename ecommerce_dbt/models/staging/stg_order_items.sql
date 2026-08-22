WITH source AS (
    SELECT *
    FROM {{ source('raw', 'raw_order_items') }}
),

renamed AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        _source_file,
        _ingestion_timestamp,
        _pipeline_run_id

    FROM source
)

SELECT *
FROM renamed