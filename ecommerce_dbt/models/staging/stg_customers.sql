WITH source AS (
    SELECT *
    FROM {{ source('raw', 'raw_customers') }}
),

renamed AS (
    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        Lower(TRIM(email)) AS email,
        TRIM(country) as country,
        created_at,
        _source_file,
        _ingestion_timestamp,
        _pipeline_run_id

    FROM source
)

SELECT *
FROM renamed