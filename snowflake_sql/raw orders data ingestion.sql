USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

-- Define table and it's schema

CREATE OR REPLACE TABLE RAW_ORDERS(
    order_id INT,
    customer_id INT,
    order_date DATE,
    status VARCHAR,

    _source_file VARCHAR,
    _ingestion_timestamp TIMESTAMP_NTZ,
    _pipeline_run_id VARCHAR
);

-- Inspect staged data before load into the raw layer

SELECT
    $1 AS order_id,
    $2 AS customer_id,
    $3 AS order_date,
    $4 AS status
FROM @ECOMMERCE_RAW_STAGE/orders.csv
(FILE_FORMAT => 'CSV_FORMAT');

-- Load data into the table

COPY INTO RAW_ORDERS(
    order_id,
    customer_id,
    order_date,
    status,
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
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        'run_001'
    FROM @ECOMMERCE_RAW_STAGE/orders.csv
    )
FILE_FORMAT = 'CSV_FORMAT';

SELECT *
FROM ECOMMERCE_DB.RAW.RAW_ORDERS;
