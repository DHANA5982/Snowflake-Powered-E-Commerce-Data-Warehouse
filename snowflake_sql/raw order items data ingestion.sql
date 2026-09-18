USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

-- Define table and it's schema

CREATE OR REPLACE TABLE RAW_ORDER_ITEMS(
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price NUMBER(10,2),

    _source_file VARCHAR,
    _ingestion_timestamp TIMESTAMP_NTZ,
    _pipeline_run_id VARCHAR
);

-- Inspect data before load into raw layer

SELECT
    $1 AS order_item_id,
    $2 AS order_id,
    $3 AS product_id,
    $4 AS quantity,
    $5 AS unit_price
FROM @ECOMMERCE_RAW_STAGE/order_items.csv
(FILE_FORMAT => 'CSV_FORMAT');

-- Load data into the table

COPY INTO RAW_ORDER_ITEMS(
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
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
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        'run_001'
    FROM @ECOMMERCE_RAW_STAGE/order_items.csv
    )
FILE_FORMAT = 'CSV_FORMAT';

SELECT *
FROM ECOMMERCE_DB.RAW.RAW_ORDER_ITEMS;