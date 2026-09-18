USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

-- Define table and it's schema

CREATE OR REPLACE TABLE RAW_PRODUCTS(
    product_id INTEGER,
    product_name VARCHAR,
    category VARCHAR,
    price NUMBER(10,2),

    _source_file VARCHAR,
    _ingestion_timestamp TIMESTAMP_NTZ,
    _pipeline_run_id VARCHAR
);

-- Inspect staged data before load into the Raw layer

SELECT
    $1 AS product_id,
    $2 AS product_name,
    $3 AS category,
    $4 AS price
FROM @ECOMMERCE_RAW_STAGE/products.csv
(FILE_FORMAT => 'CSV_FORMAT');

-- Load the data into the table

COPY INTO RAW_PRODUCTS(
    product_id,
    product_name,
    category,
    price,
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
        'run_oo1'
    FROM @ECOMMERCE_RAW_STAGE/products.csv
    )
FILE_FORMAT = 'CSV_FORMAT';

SELECT COUNT(*)
FROM ECOMMERCE_DB.RAW.RAW_PRODUCTS;

SELECT *
FROM ECOMMERCE_DB.RAW.RAW_PRODUCTS;