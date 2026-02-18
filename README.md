# Unified Query Engine Prototype

A prototype demonstrating DuckDB as a unified query engine across multiple data sources:
- **PostgreSQL** - Transactional/operational data
- **Apache Iceberg** - Analytical data lake tables (stored in MinIO/S3)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DuckDB                               │
│                 (Unified Query Engine)                      │
├─────────────────────────────────────────────────────────────┤
│  postgres extension  │  iceberg extension  │  httpfs ext    │
└──────────┬───────────┴─────────┬───────────┴───────┬────────┘
           │                     │                   │
           ▼                     ▼                   │
    ┌──────────────┐     ┌──────────────┐           │
    │  PostgreSQL  │     │    MinIO     │◄──────────┘
    │  (customers, │     │   (Iceberg   │
    │   products)  │     │    tables)   │
    └──────────────┘     └──────────────┘
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager

### 1. Install Dependencies

```bash
uv sync
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

This starts:
- PostgreSQL (port 5432) - with sample customers and products
- MinIO (ports 9000/9001) - S3-compatible storage for Iceberg tables

### 3. Create Iceberg Tables

```bash
uv run python src/setup_iceberg.py
```

This creates sample `orders` and `events` tables in Iceberg format.

### 4. Run Example Queries

```bash
uv run python examples/cross_source_queries.py
```

## Data Model

### PostgreSQL Tables (Transactional)

**customers**
- id, name, email, region, tier, created_at

**products**  
- id, name, category, price, active

### Iceberg Tables (Analytical)

**orders** (s3://warehouse/analytics/orders)
- order_id, customer_id, product_id, quantity, unit_price, total_amount, status, order_date, shipped_date

**events** (s3://warehouse/analytics/events)
- event_id, customer_id, event_type, event_timestamp, page_url, session_id, device_type, country

## Usage

### Python API

```python
from src.query_engine import QueryEngine

qe = QueryEngine()

# Query PostgreSQL
customers = qe.query("SELECT * FROM postgres_db.public.customers")

# Query Iceberg
orders = qe.query(f"SELECT * FROM {qe.iceberg('orders')}")

# Cross-source JOIN
results = qe.query(f"""
    SELECT c.name, SUM(o.total_amount) as revenue
    FROM postgres_db.public.customers c
    JOIN {qe.iceberg('orders')} o ON c.id = o.customer_id
    GROUP BY c.name
    ORDER BY revenue DESC
""")
```

### Direct DuckDB CLI

```bash
# Start DuckDB CLI
uv run python -c "import duckdb; duckdb.connect().execute('.open')"

# Or use duckdb directly if installed
duckdb
```

```sql
-- Load extensions
INSTALL postgres; LOAD postgres;
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

-- Configure S3 for MinIO
CREATE SECRET minio_secret (
    TYPE s3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
);

-- Attach PostgreSQL
ATTACH 'host=localhost port=5433 dbname=warehouse user=postgres password=postgres' 
AS postgres_db (TYPE postgres);

-- Query across sources
SELECT c.name, SUM(o.total_amount) as revenue
FROM postgres_db.public.customers c
JOIN iceberg_scan('s3://warehouse/analytics/orders') o ON c.id = o.customer_id
GROUP BY c.name;
```

## Services

| Service | Port | Credentials | Purpose |
|---------|------|-------------|---------|
| PostgreSQL | 5433 | postgres/postgres | Transactional data |
| MinIO API | 9000 | minioadmin/minioadmin | S3-compatible storage |
| MinIO Console | 9001 | minioadmin/minioadmin | Web UI for MinIO |

## Development

### Project Structure

```
unified-query-engine-prototype/
├── docker-compose.yml           # PostgreSQL + MinIO
├── infrastructure/
│   └── postgres/
│       └── init.sql             # Sample PostgreSQL data
├── data/
│   └── iceberg/                 # Local Iceberg catalog DB
├── src/
│   ├── connections.py           # DuckDB connection management
│   ├── setup_iceberg.py         # Create Iceberg tables
│   └── query_engine.py          # High-level query interface
├── examples/
│   └── cross_source_queries.py  # Example queries
└── pyproject.toml               # Dependencies
```

### Useful Commands

```bash
# Start services
docker compose up -d

# Stop services
docker compose down

# View logs
docker compose logs -f

# Reset everything (removes data)
docker compose down -v
uv run python src/setup_iceberg.py

# Run tests
uv run pytest
```

## Future Extensions

This prototype is designed to be extended with:

1. **AI Query Agent** - Natural language to SQL across sources
2. **Additional Sources** - MySQL, SQLite, Delta Lake, Parquet files
3. **Query Optimization** - Pushdown filters, caching strategies
4. **Schema Discovery** - Auto-detect and catalog available tables
