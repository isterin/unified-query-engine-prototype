"""
Setup Iceberg tables in MinIO storage using PyIceberg.

This script creates Iceberg tables with sample data that can be queried
alongside PostgreSQL data using DuckDB.
"""

import random
from datetime import datetime, timedelta
from decimal import Decimal

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    DoubleType,
    BooleanType,
    NestedField,
)


# MinIO/S3 configuration
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
WAREHOUSE_PATH = "s3://warehouse"

# SQLite catalog for local Iceberg metadata (simple and works well for dev)
CATALOG_DB_PATH = "data/iceberg_catalog.db"


def create_catalog() -> SqlCatalog:
    """Create a SQL-based Iceberg catalog backed by SQLite."""
    catalog = SqlCatalog(
        "local",
        **{
            "uri": f"sqlite:///{CATALOG_DB_PATH}",
            "warehouse": WAREHOUSE_PATH,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
        },
    )
    return catalog


def create_orders_schema() -> Schema:
    """Define the schema for the orders Iceberg table."""
    return Schema(
        NestedField(1, "order_id", LongType(), required=False),
        NestedField(2, "customer_id", IntegerType(), required=False),
        NestedField(3, "product_id", IntegerType(), required=False),
        NestedField(4, "quantity", IntegerType(), required=False),
        NestedField(5, "unit_price", DoubleType(), required=False),
        NestedField(6, "total_amount", DoubleType(), required=False),
        NestedField(7, "status", StringType(), required=False),
        NestedField(8, "order_date", TimestampType(), required=False),
        NestedField(9, "shipped_date", TimestampType(), required=False),
    )


def create_events_schema() -> Schema:
    """Define the schema for the analytics events Iceberg table."""
    return Schema(
        NestedField(1, "event_id", LongType(), required=False),
        NestedField(2, "customer_id", IntegerType(), required=False),
        NestedField(3, "event_type", StringType(), required=False),
        NestedField(4, "event_timestamp", TimestampType(), required=False),
        NestedField(5, "page_url", StringType(), required=False),
        NestedField(6, "session_id", StringType(), required=False),
        NestedField(7, "device_type", StringType(), required=False),
        NestedField(8, "country", StringType(), required=False),
    )


def generate_orders_data(num_orders: int = 500) -> pa.Table:
    """Generate sample orders data with explicit PyArrow schema."""
    random.seed(42)

    statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    status_weights = [0.1, 0.15, 0.2, 0.45, 0.1]

    # Customer IDs 1-20 match our PostgreSQL customers
    customer_ids = list(range(1, 21))
    # Product IDs 1-10 match our PostgreSQL products
    product_ids = list(range(1, 11))

    # Prices for each product (matching products table)
    product_prices = {
        1: 99.99,
        2: 299.99,
        3: 999.99,
        4: 9.99,
        5: 49.99,
        6: 199.99,
        7: 29.99,
        8: 149.99,
        9: 199.99,
        10: 499.99,
    }

    base_date = datetime(2024, 1, 1)

    orders = {
        "order_id": [],
        "customer_id": [],
        "product_id": [],
        "quantity": [],
        "unit_price": [],
        "total_amount": [],
        "status": [],
        "order_date": [],
        "shipped_date": [],
    }

    for i in range(num_orders):
        order_id = 10000 + i
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        unit_price = product_prices[product_id]
        total_amount = round(unit_price * quantity, 2)
        status = random.choices(statuses, weights=status_weights)[0]

        # Random order date within the last 6 months
        days_offset = random.randint(0, 180)
        order_date = base_date + timedelta(days=days_offset)

        # Shipped date for shipped/delivered orders
        shipped_date = None
        if status in ["shipped", "delivered"]:
            shipped_date = order_date + timedelta(days=random.randint(1, 5))

        orders["order_id"].append(order_id)
        orders["customer_id"].append(customer_id)
        orders["product_id"].append(product_id)
        orders["quantity"].append(quantity)
        orders["unit_price"].append(unit_price)
        orders["total_amount"].append(total_amount)
        orders["status"].append(status)
        orders["order_date"].append(order_date)
        orders["shipped_date"].append(shipped_date)

    # Create PyArrow table with explicit schema matching Iceberg
    schema = pa.schema(
        [
            pa.field("order_id", pa.int64()),
            pa.field("customer_id", pa.int32()),
            pa.field("product_id", pa.int32()),
            pa.field("quantity", pa.int32()),
            pa.field("unit_price", pa.float64()),
            pa.field("total_amount", pa.float64()),
            pa.field("status", pa.string()),
            pa.field("order_date", pa.timestamp("us")),
            pa.field("shipped_date", pa.timestamp("us")),
        ]
    )

    return pa.Table.from_pydict(orders, schema=schema)


def generate_events_data(num_events: int = 2000) -> pa.Table:
    """Generate sample analytics events data."""
    random.seed(43)

    event_types = [
        "page_view",
        "click",
        "search",
        "add_to_cart",
        "checkout",
        "purchase",
    ]
    event_weights = [0.4, 0.25, 0.15, 0.1, 0.05, 0.05]

    pages = [
        "/",
        "/products",
        "/products/analytics",
        "/products/storage",
        "/pricing",
        "/about",
        "/contact",
        "/docs",
        "/login",
        "/signup",
    ]

    devices = ["desktop", "mobile", "tablet"]
    device_weights = [0.5, 0.4, 0.1]

    countries = ["US", "UK", "DE", "FR", "JP", "BR", "AU", "CA", "IN", "MX"]
    country_weights = [0.3, 0.12, 0.1, 0.08, 0.1, 0.08, 0.07, 0.06, 0.05, 0.04]

    customer_ids = list(range(1, 21))
    base_date = datetime(2024, 1, 1)

    events = {
        "event_id": [],
        "customer_id": [],
        "event_type": [],
        "event_timestamp": [],
        "page_url": [],
        "session_id": [],
        "device_type": [],
        "country": [],
    }

    for i in range(num_events):
        event_id = 100000 + i
        customer_id = random.choice(customer_ids)
        event_type = random.choices(event_types, weights=event_weights)[0]

        # Random timestamp within the last 6 months
        seconds_offset = random.randint(0, 180 * 24 * 60 * 60)
        event_timestamp = base_date + timedelta(seconds=seconds_offset)

        page_url = random.choice(pages)
        session_id = f"sess_{customer_id}_{random.randint(1000, 9999)}"
        device_type = random.choices(devices, weights=device_weights)[0]
        country = random.choices(countries, weights=country_weights)[0]

        events["event_id"].append(event_id)
        events["customer_id"].append(customer_id)
        events["event_type"].append(event_type)
        events["event_timestamp"].append(event_timestamp)
        events["page_url"].append(page_url)
        events["session_id"].append(session_id)
        events["device_type"].append(device_type)
        events["country"].append(country)

    # Create PyArrow table with explicit schema matching Iceberg
    schema = pa.schema(
        [
            pa.field("event_id", pa.int64()),
            pa.field("customer_id", pa.int32()),
            pa.field("event_type", pa.string()),
            pa.field("event_timestamp", pa.timestamp("us")),
            pa.field("page_url", pa.string()),
            pa.field("session_id", pa.string()),
            pa.field("device_type", pa.string()),
            pa.field("country", pa.string()),
        ]
    )

    return pa.Table.from_pydict(events, schema=schema)


def setup_iceberg_tables():
    """Create Iceberg tables and load sample data."""
    print("Setting up Iceberg catalog...")
    catalog = create_catalog()

    # Create namespace if it doesn't exist
    namespace = "analytics"
    try:
        catalog.create_namespace(namespace)
        print(f"Created namespace: {namespace}")
    except Exception as e:
        print(f"Namespace {namespace} already exists or error: {e}")

    # Create and populate orders table
    print("\nCreating orders table...")
    orders_schema = create_orders_schema()

    try:
        catalog.drop_table(f"{namespace}.orders")
    except Exception:
        pass

    orders_table = catalog.create_table(
        identifier=f"{namespace}.orders",
        schema=orders_schema,
    )

    orders_data = generate_orders_data(500)
    orders_table.append(orders_data)
    print(f"Loaded {len(orders_data)} orders into {namespace}.orders")

    # Create and populate events table
    print("\nCreating events table...")
    events_schema = create_events_schema()

    try:
        catalog.drop_table(f"{namespace}.events")
    except Exception:
        pass

    events_table = catalog.create_table(
        identifier=f"{namespace}.events",
        schema=events_schema,
    )

    events_data = generate_events_data(2000)
    events_table.append(events_data)
    print(f"Loaded {len(events_data)} events into {namespace}.events")

    # Print table locations for DuckDB access
    print("\n" + "=" * 60)
    print("Iceberg tables created successfully!")
    print("=" * 60)
    print(f"\nOrders table location: {orders_table.location()}")
    print(f"Events table location: {events_table.location()}")
    print("\nYou can now query these tables with DuckDB using iceberg_scan()")

    return catalog


if __name__ == "__main__":
    setup_iceberg_tables()
