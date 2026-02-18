# Unified Query Engine Prototype

A prototype demonstrating DuckDB as a unified query engine across **three independent data sources**:

1. **PostgreSQL** - Transactional database
2. **Iceberg Analytics Catalog** - Customer analytics data lake (separate S3 bucket)
3. **Iceberg Inventory Catalog** - Supply chain data lake (separate S3 bucket)

This simulates a real federated data environment where an AI agent would query across completely independent data systems.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                            DuckDB                                   │
│                    (Unified Query Engine)                           │
├─────────────────────────────────────────────────────────────────────┤
│    postgres extension  │  iceberg extension  │  httpfs extension    │
└────────────┬───────────┴──────────┬──────────┴──────────┬───────────┘
             │                      │                     │
             ▼                      ▼                     ▼
      ┌────────────┐    ┌─────────────────────────────────────┐
      │ PostgreSQL │    │              MinIO                  │
      │            │    │         (S3-compatible)             │
      │ customers  │    ├─────────────────┬───────────────────┤
      │ products   │    │ s3://analytics/ │  s3://inventory/  │
      └────────────┘    │                 │                   │
                        │ CATALOG A       │  CATALOG B        │
                        │ (separate DB)   │  (separate DB)    │
                        │                 │                   │
                        │ • orders        │  • suppliers      │
                        │ • events        │  • shipments      │
                        │                 │  • inventory      │
                        └─────────────────┴───────────────────┘
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager
- [Task](https://taskfile.dev) (optional, for convenience commands)

### Setup

```bash
# Full setup: install deps, start services, create all tables
task setup

# Or manually:
uv sync
docker compose up -d
uv run python src/setup_iceberg.py
```

### Run Example Queries

```bash
uv run python examples/cross_source_queries.py
```

### Management Commands

```bash
task setup    # Full initialization
task reset    # Destroy all data (Postgres + both Iceberg catalogs)
task up       # Start Docker services
task down     # Stop Docker services
```

## Data Sources

### 1. PostgreSQL Database (Transactional)

| Table | Rows | Description |
|-------|------|-------------|
| `customers` | 10,000 | Customer master data |
| `products` | 10 | Product catalog |

### 2. Iceberg Analytics Catalog

**Metadata**: `data/analytics_catalog.db`  
**Storage**: `s3://analytics/`

| Table | Rows | Path |
|-------|------|------|
| `orders` | 2,000,000 | `s3://analytics/default/orders` |
| `events` | 10,000,000 | `s3://analytics/default/events` |

### 3. Iceberg Inventory Catalog

**Metadata**: `data/inventory_catalog.db`  
**Storage**: `s3://inventory/`

| Table | Rows | Path |
|-------|------|------|
| `suppliers` | 10 | `s3://inventory/default/suppliers` |
| `inventory_levels` | 50 | `s3://inventory/default/inventory_levels` |
| `shipments` | ~1,500,000 | `s3://inventory/default/shipments` |

## Example Queries

### Query Single Source

```python
from src.query_engine import QueryEngine
qe = QueryEngine()

# PostgreSQL
customers = qe.query("SELECT * FROM postgres_db.public.customers LIMIT 10")

# Iceberg Analytics catalog
orders = qe.query(f"SELECT * FROM {qe.iceberg('orders')} LIMIT 10")

# Iceberg Inventory catalog  
suppliers = qe.query(f"SELECT * FROM {qe.iceberg('suppliers')}")
```

### Cross-Source JOIN (PostgreSQL + Iceberg Analytics)

```python
result = qe.query(f"""
    SELECT c.name, c.region, SUM(o.total_amount) as revenue
    FROM postgres_db.public.customers c
    JOIN {qe.iceberg('orders')} o ON c.id = o.customer_id
    GROUP BY c.name, c.region
    ORDER BY revenue DESC
""")
```

### Cross-Catalog JOIN (Two Iceberg Catalogs)

```python
result = qe.query(f"""
    SELECT 
        sup.name as supplier,
        COUNT(s.shipment_id) as shipments
    FROM {qe.iceberg('suppliers')} sup
    JOIN {qe.iceberg('shipments')} s ON sup.supplier_id = s.supplier_id
    GROUP BY sup.name
""")
```

### Three-Source JOIN (PostgreSQL + Both Iceberg Catalogs)

```python
result = qe.query(f"""
    SELECT 
        c.region,
        p.category,
        sup.country as supplier_country,
        COUNT(DISTINCT o.order_id) as orders,
        SUM(o.total_amount) as revenue
    FROM postgres_db.public.customers c
    JOIN {qe.iceberg('orders')} o ON c.id = o.customer_id
    JOIN postgres_db.public.products p ON o.product_id = p.id
    JOIN {qe.iceberg('shipments')} s ON o.order_id = s.order_id
    JOIN {qe.iceberg('suppliers')} sup ON s.supplier_id = sup.supplier_id
    GROUP BY c.region, p.category, sup.country
""")
```

## Services

| Service | Port | Credentials | Purpose |
|---------|------|-------------|---------|
| PostgreSQL | 5433 | postgres/postgres | Transactional data |
| MinIO API | 9000 | minioadmin/minioadmin | S3-compatible storage |
| MinIO Console | 9001 | minioadmin/minioadmin | Web UI for MinIO |

## Project Structure

```
unified-query-engine-prototype/
├── Taskfile.yml                 # Task runner commands
├── docker-compose.yml           # PostgreSQL + MinIO (2 buckets)
├── infrastructure/
│   └── postgres/
│       └── init.sql             # 10,000 customers + 10 products
├── data/
│   ├── analytics_catalog.db     # Iceberg Analytics metadata
│   └── inventory_catalog.db     # Iceberg Inventory metadata
├── src/
│   ├── connections.py           # DuckDB connection management
│   ├── setup_iceberg.py         # Create both Iceberg catalogs
│   └── query_engine.py          # High-level query interface
├── examples/
│   └── cross_source_queries.py  # 11 cross-source query examples
└── pyproject.toml               # uv dependencies
```

## Key Design Decisions

### Two Separate Iceberg Catalogs

The Analytics and Inventory data live in **completely independent Iceberg catalogs**:

- Separate SQLite metadata databases
- Separate S3 buckets
- No shared state

This accurately simulates querying across independent data systems (e.g., a company's analytics warehouse vs. their supply chain system).

### DuckDB as Federation Layer

DuckDB reads Iceberg tables directly via S3 paths using `iceberg_scan()`. It doesn't need access to the Iceberg catalog metadata - it reads the Iceberg manifest files directly from S3. This makes it ideal for federating queries across any Iceberg-compatible data lake.

## Future Extensions

1. **AI Query Agent** - Natural language to SQL across all three sources
2. **Additional Sources** - MySQL, Delta Lake, Parquet files
3. **Query Optimization** - Pushdown filters, caching
4. **Schema Discovery** - Auto-detect available tables across sources
