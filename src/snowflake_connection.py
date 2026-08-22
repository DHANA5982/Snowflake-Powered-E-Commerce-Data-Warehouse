import os

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_connection():
    return snowflake.connector.connect(
        account = os.getenv('SNOWFLAKE_ACCOUNT'),
        user = os.getenv('SNOWFLAKE_USER'),
        password = os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
        database = os.getenv('SNOWFLAKE_DATABASE'),
        schema = os.getenv('SNOWFLAKE_SCHEMA')
    )

def main():
    connection = get_snowflake_connection()

    cursor = connection.cursor()

    cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")

    result = cursor.fetchone()

    print('Connected to Snowflake successfully.')
    print(f'user: {result[0]}')
    print(f'database: {result[1]}')
    print(f'schema: {result[2]}')
    

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()