"""
Setup Delta Lake tables in MinIO S3.

This script creates a Delta Lake table containing product reviews data
that can join to other data sources:

- product_reviews (50k rows)
  - order_id -> joins to Iceberg orders table
  - customer_id -> joins to PostgreSQL customers table
  - product_id -> joins to PostgreSQL products table

Prerequisites:
    - MinIO running with 'delta' bucket created
"""

import random
from datetime import datetime, timedelta

import pyarrow as pa
from deltalake import write_deltalake

# MinIO/S3 configuration
MINIO_ENDPOINT = "http://localhost:9000"
STORAGE_LOCATION = "s3://delta/product_reviews"

# Storage options for Delta Lake to access MinIO
STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true",
}

# Data generation parameters (must match setup_iceberg.py)
NUM_CUSTOMERS = 10_000
NUM_PRODUCTS = 10
NUM_ORDERS = 2_000_000
NUM_REVIEWS = 50_000

# Review generation
REVIEW_TEMPLATES = [
    "Great product!",
    "Exactly what I needed.",
    "Good value for money.",
    "Fast shipping, good quality.",
    "Works as expected.",
    "Decent product.",
    "Could be better.",
    "Not bad, not great.",
    "Average quality.",
    "Disappointed with this purchase.",
    "Product was damaged.",
    "Would not recommend.",
]

SENTIMENTS = {
    5: "positive",
    4: "positive",
    3: "neutral",
    2: "negative",
    1: "negative",
}


def generate_reviews_data() -> pa.Table:
    """Generate 50k product reviews with deterministic order_ids."""
    random.seed(100)  # Different seed from other setup scripts

    # Rating distribution: skewed toward positive reviews
    # 5 stars: 35%, 4 stars: 30%, 3 stars: 20%, 2 stars: 10%, 1 star: 5%
    rating_weights = [0.05, 0.10, 0.20, 0.30, 0.35]

    # Base dates
    base_order_date = datetime(2023, 1, 1)
    date_range_days = 730  # 2 years

    reviews = {
        "review_id": [],
        "order_id": [],
        "customer_id": [],
        "product_id": [],
        "rating": [],
        "review_text": [],
        "sentiment": [],
        "review_date": [],
        "helpful_votes": [],
    }

    # Sample order IDs - use same algorithm as setup_iceberg.py
    # Orders start at 10000 and go up to 10000 + NUM_ORDERS
    # We'll sample ~2.5% of orders to have reviews
    sampled_order_indices = random.sample(range(NUM_ORDERS), NUM_REVIEWS)

    for i, order_idx in enumerate(sampled_order_indices):
        review_id = 500000 + i
        order_id = 10000 + order_idx

        # Generate customer_id and product_id using same logic as orders
        # (deterministic based on order_idx)
        random.seed(42 + order_idx)  # Same seed pattern as orders
        customer_id = random.randint(1, NUM_CUSTOMERS)
        product_id = random.randint(1, NUM_PRODUCTS)

        # Reset to our review seed for review-specific data
        random.seed(100 + i)

        rating = random.choices([1, 2, 3, 4, 5], weights=rating_weights)[0]
        sentiment = SENTIMENTS[rating]

        # Select review text based on rating
        if rating >= 4:
            review_text = random.choice(REVIEW_TEMPLATES[:5])
        elif rating == 3:
            review_text = random.choice(REVIEW_TEMPLATES[5:9])
        else:
            review_text = random.choice(REVIEW_TEMPLATES[9:])

        # Review date is 1-30 days after order date
        order_days_offset = order_idx % date_range_days
        order_date = base_order_date + timedelta(days=order_days_offset)
        review_date = order_date + timedelta(days=random.randint(1, 30))

        # Helpful votes - higher rated reviews tend to get more votes
        helpful_votes = random.randint(0, rating * 10)

        reviews["review_id"].append(review_id)
        reviews["order_id"].append(order_id)
        reviews["customer_id"].append(customer_id)
        reviews["product_id"].append(product_id)
        reviews["rating"].append(rating)
        reviews["review_text"].append(review_text)
        reviews["sentiment"].append(sentiment)
        reviews["review_date"].append(review_date)
        reviews["helpful_votes"].append(helpful_votes)

    schema = pa.schema(
        [
            pa.field("review_id", pa.int64(), nullable=False),
            pa.field("order_id", pa.int64(), nullable=False),
            pa.field("customer_id", pa.int32(), nullable=False),
            pa.field("product_id", pa.int32(), nullable=False),
            pa.field("rating", pa.int32(), nullable=False),
            pa.field("review_text", pa.string(), nullable=True),
            pa.field("sentiment", pa.string(), nullable=False),
            pa.field("review_date", pa.timestamp("us"), nullable=False),
            pa.field("helpful_votes", pa.int32(), nullable=False),
        ]
    )

    return pa.Table.from_pydict(reviews, schema=schema)


def write_delta_table(table: pa.Table) -> None:
    """Write the PyArrow table as a Delta Lake table to MinIO."""
    print(f"Writing Delta table to {STORAGE_LOCATION}...")

    write_deltalake(
        STORAGE_LOCATION,
        table,
        mode="overwrite",
        storage_options=STORAGE_OPTIONS,
    )

    print(f"  Written {len(table):,} rows to Delta table")


def setup_delta():
    """Set up Delta Lake product_reviews table."""
    print("=" * 70)
    print("  DELTA LAKE SETUP")
    print("  Storage:", STORAGE_LOCATION)
    print("=" * 70)

    # Step 1: Generate reviews data
    print(f"\n1. Generating {NUM_REVIEWS:,} product reviews...")
    reviews_table = generate_reviews_data()

    # Step 2: Write Delta table
    print("\n2. Writing Delta Lake table to MinIO...")
    write_delta_table(reviews_table)

    # Summary
    print("\n" + "=" * 70)
    print("  DELTA LAKE SETUP COMPLETE")
    print("=" * 70)
    print(f"\nTable: product_reviews ({NUM_REVIEWS:,} rows)")
    print(f"Storage: {STORAGE_LOCATION}")
    print("\nTable schema:")
    print("  - review_id: BIGINT (PK)")
    print("  - order_id: BIGINT (FK -> orders)")
    print("  - customer_id: INT (FK -> customers)")
    print("  - product_id: INT (FK -> products)")
    print("  - rating: INT (1-5)")
    print("  - review_text: STRING")
    print("  - sentiment: STRING (positive/neutral/negative)")
    print("  - review_date: TIMESTAMP")
    print("  - helpful_votes: INT")
    print("\nQuery with DuckDB:")
    print(f"  SELECT * FROM delta_scan('{STORAGE_LOCATION}') LIMIT 10;")


if __name__ == "__main__":
    setup_delta()
