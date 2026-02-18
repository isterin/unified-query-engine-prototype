"""
Setup Iceberg tables in MinIO storage using PyIceberg.

This script creates TWO SEPARATE Iceberg catalogs with sample data:

1. Analytics Catalog (data/analytics_catalog.db)
   - Bucket: s3://analytics/
   - Tables: orders (2M rows), events (10M rows)

2. Inventory Catalog (data/inventory_catalog.db)
   - Bucket: s3://inventory/
   - Tables: suppliers, inventory_levels, shipments (~1.5M rows)

Data is written in batches to manage memory usage.
"""

import random
from datetime import datetime, timedelta
from typing import Dict, List

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    DoubleType,
    NestedField,
)


# MinIO/S3 configuration
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Separate catalog configurations
ANALYTICS_CATALOG_DB = "data/analytics_catalog.db"
ANALYTICS_WAREHOUSE = "s3://analytics"

INVENTORY_CATALOG_DB = "data/inventory_catalog.db"
INVENTORY_WAREHOUSE = "s3://inventory"

# Data sizes - SCALED UP
NUM_CUSTOMERS = 10_000  # Must match PostgreSQL
NUM_PRODUCTS = 10  # Must match PostgreSQL
NUM_ORDERS = 2_000_000  # 2 million orders
NUM_EVENTS = 10_000_000  # 10 million events
NUM_SUPPLIERS = 10

# Batch sizes for memory management
BATCH_SIZE_ORDERS = 200_000  # 200k per batch
BATCH_SIZE_EVENTS = 500_000  # 500k per batch
BATCH_SIZE_SHIPMENTS = 200_000


def create_analytics_catalog() -> SqlCatalog:
    """Create the Analytics Iceberg catalog (separate from Inventory)."""
    return SqlCatalog(
        "analytics",
        **{
            "uri": f"sqlite:///{ANALYTICS_CATALOG_DB}",
            "warehouse": ANALYTICS_WAREHOUSE,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
        },
    )


def create_inventory_catalog() -> SqlCatalog:
    """Create the Inventory Iceberg catalog (separate from Analytics)."""
    return SqlCatalog(
        "inventory",
        **{
            "uri": f"sqlite:///{INVENTORY_CATALOG_DB}",
            "warehouse": INVENTORY_WAREHOUSE,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
        },
    )


# =============================================================================
# SCHEMAS
# =============================================================================


def create_orders_schema() -> Schema:
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


def create_suppliers_schema() -> Schema:
    return Schema(
        NestedField(1, "supplier_id", IntegerType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "country", StringType(), required=False),
        NestedField(4, "rating", DoubleType(), required=False),
        NestedField(5, "contact_email", StringType(), required=False),
    )


def create_inventory_levels_schema() -> Schema:
    return Schema(
        NestedField(1, "product_id", IntegerType(), required=False),
        NestedField(2, "warehouse_location", StringType(), required=False),
        NestedField(3, "quantity_on_hand", IntegerType(), required=False),
        NestedField(4, "reorder_point", IntegerType(), required=False),
        NestedField(5, "last_updated", TimestampType(), required=False),
    )


def create_shipments_schema() -> Schema:
    return Schema(
        NestedField(1, "shipment_id", LongType(), required=False),
        NestedField(2, "order_id", LongType(), required=False),
        NestedField(3, "supplier_id", IntegerType(), required=False),
        NestedField(4, "product_id", IntegerType(), required=False),
        NestedField(5, "quantity", IntegerType(), required=False),
        NestedField(6, "status", StringType(), required=False),
        NestedField(7, "shipped_date", TimestampType(), required=False),
        NestedField(8, "delivered_date", TimestampType(), required=False),
    )


# =============================================================================
# PYARROW SCHEMAS
# =============================================================================

ORDERS_PA_SCHEMA = pa.schema(
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

EVENTS_PA_SCHEMA = pa.schema(
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

SHIPMENTS_PA_SCHEMA = pa.schema(
    [
        pa.field("shipment_id", pa.int64()),
        pa.field("order_id", pa.int64()),
        pa.field("supplier_id", pa.int32()),
        pa.field("product_id", pa.int32()),
        pa.field("quantity", pa.int32()),
        pa.field("status", pa.string()),
        pa.field("shipped_date", pa.timestamp("us")),
        pa.field("delivered_date", pa.timestamp("us")),
    ]
)


# =============================================================================
# BATCHED DATA GENERATORS
# =============================================================================


def generate_orders_batch(start_idx: int, batch_size: int, seed: int) -> pa.Table:
    """Generate a batch of orders data."""
    random.seed(seed + start_idx)

    statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    status_weights = [0.05, 0.10, 0.15, 0.60, 0.10]

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

    base_date = datetime(2023, 1, 1)
    date_range_days = 730

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

    for i in range(batch_size):
        order_id = 10000 + start_idx + i
        customer_id = random.randint(1, NUM_CUSTOMERS)
        product_id = random.randint(1, NUM_PRODUCTS)
        quantity = random.randint(1, 5)
        unit_price = product_prices[product_id]
        total_amount = round(unit_price * quantity, 2)
        status = random.choices(statuses, weights=status_weights)[0]

        days_offset = random.randint(0, date_range_days)
        order_date = base_date + timedelta(days=days_offset)

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

    return pa.Table.from_pydict(orders, schema=ORDERS_PA_SCHEMA)


def generate_events_batch(start_idx: int, batch_size: int, seed: int) -> pa.Table:
    """Generate a batch of events data."""
    random.seed(seed + start_idx)

    event_types = [
        "page_view",
        "click",
        "search",
        "add_to_cart",
        "checkout",
        "purchase",
    ]
    event_weights = [0.40, 0.25, 0.15, 0.10, 0.05, 0.05]

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
    device_weights = [0.50, 0.40, 0.10]

    countries = ["US", "UK", "DE", "FR", "JP", "BR", "AU", "CA", "IN", "MX"]
    country_weights = [0.30, 0.12, 0.10, 0.08, 0.10, 0.08, 0.07, 0.06, 0.05, 0.04]

    base_date = datetime(2023, 1, 1)
    date_range_seconds = 730 * 24 * 60 * 60

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

    for i in range(batch_size):
        event_id = 100000 + start_idx + i
        customer_id = random.randint(1, NUM_CUSTOMERS)
        event_type = random.choices(event_types, weights=event_weights)[0]

        seconds_offset = random.randint(0, date_range_seconds)
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

    return pa.Table.from_pydict(events, schema=EVENTS_PA_SCHEMA)


def generate_suppliers_data() -> pa.Table:
    """Generate supplier master data."""
    random.seed(44)

    supplier_names = [
        "Global Tech Supplies",
        "Pacific Components",
        "Euro Parts Direct",
        "Atlas Manufacturing",
        "Prime Logistics",
        "Summit Distribution",
        "Apex Industrial",
        "Vertex Solutions",
        "Pinnacle Wholesale",
        "Horizon Trading Co",
    ]

    countries = [
        "USA",
        "China",
        "Germany",
        "Japan",
        "UK",
        "Canada",
        "Mexico",
        "India",
        "Brazil",
        "Australia",
    ]

    suppliers = {
        "supplier_id": [],
        "name": [],
        "country": [],
        "rating": [],
        "contact_email": [],
    }

    for i in range(NUM_SUPPLIERS):
        supplier_id = i + 1
        name = supplier_names[i] if i < len(supplier_names) else f"Supplier {i + 1}"
        country = countries[i % len(countries)]
        rating = round(random.uniform(3.0, 5.0), 1)
        contact_email = f"sales@{name.lower().replace(' ', '')}.com"

        suppliers["supplier_id"].append(supplier_id)
        suppliers["name"].append(name)
        suppliers["country"].append(country)
        suppliers["rating"].append(rating)
        suppliers["contact_email"].append(contact_email)

    schema = pa.schema(
        [
            pa.field("supplier_id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field("country", pa.string()),
            pa.field("rating", pa.float64()),
            pa.field("contact_email", pa.string()),
        ]
    )

    return pa.Table.from_pydict(suppliers, schema=schema)


def generate_inventory_levels_data() -> pa.Table:
    """Generate inventory levels for each product at multiple warehouse locations."""
    random.seed(45)

    warehouses = ["US-East", "US-West", "EU-Central", "APAC-Tokyo", "APAC-Sydney"]
    last_updated = datetime(2024, 12, 1)

    inventory = {
        "product_id": [],
        "warehouse_location": [],
        "quantity_on_hand": [],
        "reorder_point": [],
        "last_updated": [],
    }

    for product_id in range(1, NUM_PRODUCTS + 1):
        for warehouse in warehouses:
            quantity = random.randint(0, 500)
            reorder_point = random.randint(50, 150)

            inventory["product_id"].append(product_id)
            inventory["warehouse_location"].append(warehouse)
            inventory["quantity_on_hand"].append(quantity)
            inventory["reorder_point"].append(reorder_point)
            inventory["last_updated"].append(
                last_updated + timedelta(hours=random.randint(-48, 0))
            )

    schema = pa.schema(
        [
            pa.field("product_id", pa.int32()),
            pa.field("warehouse_location", pa.string()),
            pa.field("quantity_on_hand", pa.int32()),
            pa.field("reorder_point", pa.int32()),
            pa.field("last_updated", pa.timestamp("us")),
        ]
    )

    return pa.Table.from_pydict(inventory, schema=schema)


def generate_shipments_for_orders(
    order_ids: List[int],
    product_ids: List[int],
    quantities: List[int],
    statuses: List[str],
    order_dates: List[datetime],
    start_shipment_id: int,
    seed: int,
) -> pa.Table:
    """Generate shipments for a batch of orders."""
    random.seed(seed)

    shipments = {
        "shipment_id": [],
        "order_id": [],
        "supplier_id": [],
        "product_id": [],
        "quantity": [],
        "status": [],
        "shipped_date": [],
        "delivered_date": [],
    }

    shipment_id = start_shipment_id
    for i, order_status in enumerate(statuses):
        if order_status in ["shipped", "delivered"]:
            order_date = order_dates[i]
            if order_date is None:
                continue

            if hasattr(order_date, "as_py"):
                order_date = order_date.as_py()

            shipped_date = order_date + timedelta(days=random.randint(1, 3))

            if order_status == "delivered":
                shipment_status = "delivered"
                delivered_date = shipped_date + timedelta(days=random.randint(1, 7))
            else:
                shipment_status = random.choices(
                    ["in_transit", "delivered"], weights=[0.7, 0.3]
                )[0]
                delivered_date = (
                    None
                    if shipment_status == "in_transit"
                    else shipped_date + timedelta(days=random.randint(1, 7))
                )

            shipments["shipment_id"].append(shipment_id)
            shipments["order_id"].append(order_ids[i])
            shipments["supplier_id"].append(random.randint(1, NUM_SUPPLIERS))
            shipments["product_id"].append(product_ids[i])
            shipments["quantity"].append(quantities[i])
            shipments["status"].append(shipment_status)
            shipments["shipped_date"].append(shipped_date)
            shipments["delivered_date"].append(delivered_date)

            shipment_id += 1

    return pa.Table.from_pydict(shipments, schema=SHIPMENTS_PA_SCHEMA), shipment_id


# =============================================================================
# SETUP FUNCTIONS
# =============================================================================


def setup_analytics_catalog():
    """
    Setup the ANALYTICS Iceberg catalog with batched writes.
    """
    print(f"\n{'=' * 70}")
    print("  ANALYTICS CATALOG")
    print(f"  Metadata: {ANALYTICS_CATALOG_DB}")
    print(f"  Storage:  {ANALYTICS_WAREHOUSE}")
    print(f"{'=' * 70}")

    catalog = create_analytics_catalog()
    namespace = "default"

    try:
        catalog.create_namespace(namespace)
        print(f"Created namespace: {namespace}")
    except Exception:
        print(f"Namespace {namespace} already exists")

    # Orders - batched writes
    print(
        f"\nCreating orders table ({NUM_ORDERS:,} rows in batches of {BATCH_SIZE_ORDERS:,})..."
    )
    try:
        catalog.drop_table(f"{namespace}.orders")
    except Exception:
        pass

    orders_table = catalog.create_table(
        identifier=f"{namespace}.orders", schema=create_orders_schema()
    )

    total_written = 0
    all_order_data = []  # Store for shipments generation

    for batch_start in range(0, NUM_ORDERS, BATCH_SIZE_ORDERS):
        batch_size = min(BATCH_SIZE_ORDERS, NUM_ORDERS - batch_start)
        batch_data = generate_orders_batch(batch_start, batch_size, seed=42)
        orders_table.append(batch_data)
        total_written += batch_size

        # Store data for shipments (we need order_id, product_id, quantity, status, order_date)
        all_order_data.append(
            {
                "order_ids": batch_data.column("order_id").to_pylist(),
                "product_ids": batch_data.column("product_id").to_pylist(),
                "quantities": batch_data.column("quantity").to_pylist(),
                "statuses": batch_data.column("status").to_pylist(),
                "order_dates": batch_data.column("order_date").to_pylist(),
            }
        )

        print(
            f"  Written {total_written:,} / {NUM_ORDERS:,} orders ({100 * total_written / NUM_ORDERS:.0f}%)"
        )

    print(f"  Loaded {total_written:,} rows into {namespace}.orders")

    # Events - batched writes
    print(
        f"\nCreating events table ({NUM_EVENTS:,} rows in batches of {BATCH_SIZE_EVENTS:,})..."
    )
    try:
        catalog.drop_table(f"{namespace}.events")
    except Exception:
        pass

    events_table = catalog.create_table(
        identifier=f"{namespace}.events", schema=create_events_schema()
    )

    total_written = 0
    for batch_start in range(0, NUM_EVENTS, BATCH_SIZE_EVENTS):
        batch_size = min(BATCH_SIZE_EVENTS, NUM_EVENTS - batch_start)
        batch_data = generate_events_batch(batch_start, batch_size, seed=43)
        events_table.append(batch_data)
        total_written += batch_size
        print(
            f"  Written {total_written:,} / {NUM_EVENTS:,} events ({100 * total_written / NUM_EVENTS:.0f}%)"
        )

    print(f"  Loaded {total_written:,} rows into {namespace}.events")

    return all_order_data


def setup_inventory_catalog(all_order_data: List[Dict]):
    """
    Setup the INVENTORY Iceberg catalog with batched writes.
    """
    print(f"\n{'=' * 70}")
    print("  INVENTORY CATALOG")
    print(f"  Metadata: {INVENTORY_CATALOG_DB}")
    print(f"  Storage:  {INVENTORY_WAREHOUSE}")
    print(f"{'=' * 70}")

    catalog = create_inventory_catalog()
    namespace = "default"

    try:
        catalog.create_namespace(namespace)
        print(f"Created namespace: {namespace}")
    except Exception:
        print(f"Namespace {namespace} already exists")

    # Suppliers
    print("\nCreating suppliers table...")
    try:
        catalog.drop_table(f"{namespace}.suppliers")
    except Exception:
        pass

    suppliers_table = catalog.create_table(
        identifier=f"{namespace}.suppliers", schema=create_suppliers_schema()
    )
    suppliers_data = generate_suppliers_data()
    suppliers_table.append(suppliers_data)
    print(f"  Loaded {len(suppliers_data):,} rows into {namespace}.suppliers")

    # Inventory Levels
    print("\nCreating inventory_levels table...")
    try:
        catalog.drop_table(f"{namespace}.inventory_levels")
    except Exception:
        pass

    inventory_table = catalog.create_table(
        identifier=f"{namespace}.inventory_levels",
        schema=create_inventory_levels_schema(),
    )
    inventory_data = generate_inventory_levels_data()
    inventory_table.append(inventory_data)
    print(f"  Loaded {len(inventory_data):,} rows into {namespace}.inventory_levels")

    # Shipments - batched based on order batches
    print(f"\nCreating shipments table (generating from {NUM_ORDERS:,} orders)...")
    try:
        catalog.drop_table(f"{namespace}.shipments")
    except Exception:
        pass

    shipments_table = catalog.create_table(
        identifier=f"{namespace}.shipments", schema=create_shipments_schema()
    )

    total_shipments = 0
    shipment_id = 200000

    for batch_idx, order_batch in enumerate(all_order_data):
        shipments_batch, shipment_id = generate_shipments_for_orders(
            order_ids=order_batch["order_ids"],
            product_ids=order_batch["product_ids"],
            quantities=order_batch["quantities"],
            statuses=order_batch["statuses"],
            order_dates=order_batch["order_dates"],
            start_shipment_id=shipment_id,
            seed=46 + batch_idx,
        )

        if len(shipments_batch) > 0:
            shipments_table.append(shipments_batch)
            total_shipments += len(shipments_batch)

        print(
            f"  Written {total_shipments:,} shipments (batch {batch_idx + 1}/{len(all_order_data)})"
        )

    print(f"  Loaded {total_shipments:,} rows into {namespace}.shipments")


def setup_iceberg_tables():
    """Create both Iceberg catalogs with sample data."""
    print("Setting up TWO SEPARATE Iceberg catalogs with LARGE datasets...")
    print(f"  - Orders: {NUM_ORDERS:,} rows")
    print(f"  - Events: {NUM_EVENTS:,} rows")
    print(f"  - Shipments: ~{int(NUM_ORDERS * 0.75):,} rows (75% of orders)")
    print("\nThis may take a few minutes...")

    # Setup both catalogs
    all_order_data = setup_analytics_catalog()
    setup_inventory_catalog(all_order_data)

    # Summary
    print(f"\n{'=' * 70}")
    print("  SETUP COMPLETE - Two Separate Iceberg Catalogs")
    print(f"{'=' * 70}")
    print("\n1. ANALYTICS CATALOG (s3://analytics/)")
    print(f"   Metadata: {ANALYTICS_CATALOG_DB}")
    print("   Tables:")
    print(f"     - default.orders  ({NUM_ORDERS:,} rows)")
    print(f"     - default.events  ({NUM_EVENTS:,} rows)")
    print("\n2. INVENTORY CATALOG (s3://inventory/)")
    print(f"   Metadata: {INVENTORY_CATALOG_DB}")
    print("   Tables:")
    print("     - default.suppliers")
    print("     - default.inventory_levels")
    print(f"     - default.shipments (~{int(NUM_ORDERS * 0.75):,} rows)")
    print("\nDuckDB can now query across PostgreSQL + both Iceberg catalogs!")


if __name__ == "__main__":
    setup_iceberg_tables()
