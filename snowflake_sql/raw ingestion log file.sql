USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

-- Define RAW layer table ingestion log table

CREATE OR REPLACE TABLE INGESTION_FILE_LOG(
    file_name VARCHAR,
    file_hash VARCHAR,
    target_table VARCHAR,
    pipeline_run_id VARCHAR,
    status VARCHAR,
    rows_loaded INT,
    ingestion_timestamp TIMESTAMP_NTZ,
    error_message VARCHAR
);

DESC TABLE INGESTION_FILE_LOG;