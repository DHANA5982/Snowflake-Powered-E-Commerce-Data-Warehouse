from pathlib import Path
import pandas as pd
from src.validation.data_quality import (check_not_null, check_unique, check_positive, check_accepted_values)



# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Raw data directory
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def load_csv(file_name: str) -> pd.DataFrame:
    """
    Load a CSV file from the raw data directory.
    """

    file_path = RAW_DATA_DIR / file_name

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError(f"File contains no data: {file_name}")

    print(f"Loaded {file_name}: {len(df)} records")

    return df

def validate_data(
        customers: pd.DataFrame,
        products: pd.DataFrame,
        orders: pd.DataFrame,
        order_items: pd.DataFrame
) -> None:

    # Customers
    check_not_null(customers, "customer_id")
    check_unique(customers, "customer_id")
    check_not_null(customers, "email")

    # Products
    check_not_null(products, "product_id")
    check_unique(products, "product_id")
    check_positive(products, "price")

    # Orders
    check_not_null(orders, "order_id")
    check_unique(orders, "order_id")
    check_not_null(orders, "customer_id")
    check_accepted_values(orders, "status", ["completed", "cancelled", "pending"])

    # order_items
    check_not_null(order_items, 'order_id')
    check_unique(order_items, 'order_item_id')
    check_not_null(order_items, 'product_id')
    check_positive(order_items, "quantity")
    check_positive(order_items, 'unit_price')

    print("\nAll data quality check passed.")


def main():
    customers = load_csv("customers.csv")
    products = load_csv("products.csv")
    orders = load_csv("orders.csv")
    order_items = load_csv("order_items.csv")

    print("\nData ingestion completed successfully.")

    validate_data(customers, products, orders, order_items)

    print("\nCustomers:")
    print(customers.head(1))

    print("\nProducts:")
    print(products.head(1))

    print("\nOrders:")
    print(orders.head(1))

    print("\nOrder Items:")
    print(order_items.head(1))


if __name__ == "__main__":
    main()
