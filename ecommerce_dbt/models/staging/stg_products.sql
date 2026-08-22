WITH source AS (

    SELECT *
    FROM {{ source('raw', 'raw_products')}}
),

renamed AS (
    SELECT
        product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        price,
        _source_file,
        _ingestion_timestamp,
        _pipeline_run_id

    FROM source
)

SELECT *
FROM renamed
