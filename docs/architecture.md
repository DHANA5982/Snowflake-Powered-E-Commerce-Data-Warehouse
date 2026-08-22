# E-Commerce Data Warehouse Architecture

## Source Layer

The project receives e-commerce data from multiple sources:

- CSV files
- REST APIs
- Relational databases

## Ingestion Layer

Python is responsible for:

- Extracting source data
- Basic validation
- Preprocessing
- Loading data into the RAW layer

## RAW Layer

The RAW layer preserves source data with minimal transformation.

Tables:

- RAW_CUSTOMERS
- RAW_PRODUCTS
- RAW_ORDERS
- RAW_ORDER_ITEMS

Technical metadata is added for auditing and traceability.

## Transformation Layer

dbt will transform RAW data through:

RAW → STAGING → INTERMEDIATE → MARTS

## Data Warehouse

The MART layer will implement a Star Schema consisting of:

- DIM_CUSTOMER
- DIM_PRODUCT
- DIM_DATE
- FACT_SALES

## Orchestration

Apache Airflow will orchestrate:

1. Data ingestion
2. RAW loading
3. dbt transformations
4. dbt tests
5. Monitoring and error handling