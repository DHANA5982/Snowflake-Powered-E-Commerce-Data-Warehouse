import os
import uuid
import hashlib
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

from src.snowflake_connection import get_snowflake_connection

load_dotenv()


TABLE_CONFIG = {
    "customers.csv": {
        "target_table": "RAW_CUSTOMERS",
        "columns": [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "country",
            "created_at",
        ],
    },

    "products.csv": {
        "target_table": "RAW_PRODUCTS",
        "columns": [
            "product_id",
            "product_name",
            "category",
            "price",
        ],
    },

    "orders.csv": {
        "target_table": "RAW_ORDERS",
        "columns": [
            "order_id",
            "customer_id",
            "order_date",
            "status",
        ],
    },

    "order_items.csv": {
        "target_table": "RAW_ORDER_ITEMS",
        "columns": [
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
        ],
    },
}


def get_files_to_process(data_directory):
    return[
        file for file in Path(data_directory).glob("*.csv")
        if file.name in TABLE_CONFIG
    ]


def generate_pipeline_run_id():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid.uuid4().hex[:8]

    return f"run_{timestamp}_{unique_id}"


def calculate_file_hash(file_path):
    sha256 = hashlib.sha256()

    with open(file_path, 'rb') as file:
        while chunk := file.read(8192):
            sha256.update(chunk)

    return sha256.hexdigest()


def check_file_already_processed(connection, file_hash, target_table):
    cursor = connection.cursor()

    try: 
        query = """
            SELECT COUNT(*),
            FROM INGESTION_FILE_LOG
            WHERE file_hash = %s
                AND target_table = %s
                AND status = 'SUCCESS'
        """

        cursor.execute(query, (file_hash, target_table))

        count = cursor.fetchone()[0]

        return count > 0

    finally:
        cursor.close()


def upload_file_to_stage(connection, file_path, stage_name):

    cursor = connection.cursor()

    try:
        absolute_path = os.path.abspath(file_path)

        put_sql = f"""
            PUT 'file://{absolute_path.replace("\\", "/")}'
            @{stage_name}
            AUTO_COMPRESS = FALSE
            OVERWRITE = TRUE
        """

        print(f"Uploading: {file_path}")
        print(f"Target stage: {stage_name}")

        cursor.execute(put_sql)

        results = cursor.fetchall()

        for row in results:
            print(row)

        return results

    finally:
        cursor.close()


def copy_into_raw(
    connection,
    stage_name,
    file_name,
    target_table,
    columns,
    pipeline_run_id,
):
    cursor = connection.cursor()

    try:
        source_columns = ",\n                    ".join(
            f"${index}"
            for index in range(1, len(columns) + 1)
        )

        target_columns = ",\n                ".join(columns)

        copy_sql = f"""
            COPY INTO {target_table}
            (
                {target_columns},
                _source_file,
                _ingestion_timestamp,
                _pipeline_run_id
            )
            FROM (
                SELECT
                    {source_columns},
                    METADATA$FILENAME,
                    CURRENT_TIMESTAMP(),
                    '{pipeline_run_id}'
                FROM @{stage_name}/{file_name}
            )
            FILE_FORMAT = "CSV_FORMAT"
        """

        print(f"Loading {file_name} into {target_table}...")

        cursor.execute(copy_sql)

        return cursor.fetchall()

    finally:
        cursor.close()


def record_ingestion(
        connection,
        file_name,
        file_hash,
        target_table,
        pipeline_run_id,
        status,
        rows_loaded,
        error_message=None):

    cursor = connection.cursor()

    try:
        query = """
            INSERT INTO INGESTION_FILE_LOG(
                file_name,
                file_hash,
                target_table,
                pipeline_run_id,
                status,
                rows_loaded,
                ingestion_timestamp,
                error_message
            )
            values(
                %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), %s
            )
        """

        cursor.execute(query, (file_name, file_hash, target_table, pipeline_run_id, status, rows_loaded, error_message))

        connection.commit()

    finally:
        cursor.close()


def main():
    
    data_directory = "data/raw"
    stage_name = "ECOMMERCE_RAW_STAGE"

    connection = get_snowflake_connection()

    try:

        files = get_files_to_process(data_directory)
        print(f"Found {len(files)} files to process.")

        for file_path in files:

            file_name = file_path.name
            config = TABLE_CONFIG[file_name]

            target_table = config["target_table"]
            columns = config["columns"]

            run_id = generate_pipeline_run_id()
            file_hash = calculate_file_hash(file_path)

            print("\n" + "=" * 60)
            print(f"Pipeline run ID: {run_id}")
            print(f"File: {file_name}")
            print(f"Target: {target_table}")
            print(f"Hash: {file_hash}")

            already_processed = check_file_already_processed(
                connection,
                file_hash,
                target_table)

            if already_processed:
                print(f"File already processed. Skipping.")
                continue

            try:
                print("New File detected.")

                upload_file_to_stage(
                    connection,
                    str(file_path),
                    stage_name,
                )

                results = copy_into_raw(
                    connection,
                    stage_name,
                    file_name,
                    target_table,
                    columns,
                    run_id,
                )

                rows_loaded = sum(
                    int(row[3])
                    for row in results
                    if len(row) > 3 and row[3] is not None
                )

                record_ingestion(
                    connection,
                    file_name,
                    file_hash,
                    target_table,
                    run_id,
                    "SUCCESS",
                    rows_loaded
                )

                print(
                    f"SUCCESS: {file_name} → "
                    f"{target_table}, ({rows_loaded} rows loaded.)"
                )
            except Exception as error:

                record_ingestion(
                    connection,
                    file_name,
                    file_hash,
                    target_table,
                    run_id,
                    "FAILED",
                    0,
                    str(error)
                )
                
                print(f"Pipeline failed: {error}")
                raise

    finally:
        connection.close()


if __name__ == "__main__":

    data_directory = "data/raw"
    stage_name = "ECOMMERCE_RAW_STAGE"

    connection = get_snowflake_connection()

    try:

        files = get_files_to_process(data_directory)
        print(f"Found {len(files)} files to process.")

        for file_path in files:

            file_name = file_path.name
            config = TABLE_CONFIG[file_name]

            target_table = config["target_table"]
            columns = config["columns"]

            run_id = generate_pipeline_run_id()
            file_hash = calculate_file_hash(file_path)

            print("\n" + "=" * 60)
            print(f"Pipeline run ID: {run_id}")
            print(f"File: {file_name}")
            print(f"Target: {target_table}")
            print(f"Hash: {file_hash}")

            already_processed = check_file_already_processed(
                connection,
                file_hash,
                target_table)

            if already_processed:
                print(f"File already processed. Skipping.")
                continue

            try:
                print("New File detected.")

                upload_file_to_stage(
                    connection,
                    str(file_path),
                    stage_name,
                )

                results = copy_into_raw(
                    connection,
                    stage_name,
                    file_name,
                    target_table,
                    columns,
                    run_id,
                )

                rows_loaded = sum(
                    int(row[3])
                    for row in results
                    if len(row) > 3 and row[3] is not None
                )

                record_ingestion(
                    connection,
                    file_name,
                    file_hash,
                    target_table,
                    run_id,
                    "SUCCESS",
                    rows_loaded
                )

                print(
                    f"SUCCESS: {file_name} → "
                    f"{target_table}, ({rows_loaded} rows loaded.)"
                )
            except Exception as error:

                record_ingestion(
                    connection,
                    file_name,
                    file_hash,
                    target_table,
                    run_id,
                    "FAILED",
                    0,
                    str(error)
                )
                
                print(f"Pipeline failed: {error}")
                raise

    finally:
        connection.close()
