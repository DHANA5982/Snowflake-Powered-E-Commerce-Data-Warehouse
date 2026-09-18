USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

-- Define table and it's schema

CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    customer_id INT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    country VARCHAR,
    created_at DATE,

    _source_file VARCHAR,
    _ingestion_timestamp TIMESTAMP_NTZ,
    _pipeline_run_id VARCHAR
);

-- Inspect staged data before load into the Raw layer

SELECT
    $1 AS customer_id,
    $2 AS first_name,
    $3 AS last_name,
    $4 AS email,
    $5 AS country,
    $6 AS created_at
FROM @ECOMMERCE_RAW_STAGE/customers.csv
(FILE_FORMAT => 'CSV_FORMAT');

-- Load the data into the table

COPY INTO RAW_CUSTOMERS(
    customer_id,
    first_name,
    last_name,
    email,
    country,
    created_at,
    _source_file,
    _ingestion_timestamp,
    _pipeline_run_id
)
FROM (
    SELECT
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        'run_001'
    FROM @ECOMMERCE_RAW_STAGE/customers.csv
    )
FILE_FORMAT = 'CSV_FORMAT';
