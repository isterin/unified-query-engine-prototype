# AGENTS.md

Guidelines for AI coding agents working in this repository.

## Project Overview

This is a **Unified Query Engine Prototype** using DuckDB to execute federated queries across:
- PostgreSQL (transactional data: customers, products)
- Two separate Iceberg catalogs stored in MinIO S3:
  - **Analytics catalog** (`s3://analytics/`): orders, events
  - **Inventory catalog** (`s3://inventory/`): suppliers, inventory_levels, shipments
- **Delta Lake** (`s3://delta/`): product_reviews

## Build Commands

```bash
# Install dependencies (uses uv for package management)
uv sync

# Full setup: install deps, start Docker, create Iceberg and Delta tables
task setup

# Start/stop Docker services (PostgreSQL + MinIO)
task up
task down

# Reset everything (removes all data)
task reset
```

## Test Commands

```bash
# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_query_engine.py

# Run a single test function
uv run pytest tests/test_query_engine.py::test_function_name

# Run tests with verbose output
uv run pytest -v

# Run tests matching a pattern
uv run pytest -k "pattern"
```

## Lint Commands

```bash
# Check code with ruff
uv run ruff check .

# Fix auto-fixable issues
uv run ruff check . --fix

# Format code with ruff
uv run ruff format .

# Check formatting without applying
uv run ruff format . --check
```

## Running Scripts

```bash
# Run the example cross-source queries
uv run python examples/cross_source_queries.py

# Run the Iceberg setup script
uv run python src/setup_iceberg.py

# Run the Delta Lake setup script
uv run python src/setup_delta.py

# Test connections
uv run python src/connections.py
```

## Code Style Guidelines

### Imports

Order imports in three groups separated by blank lines:
1. Standard library imports
2. Third-party imports
3. Local imports (relative with `.`)

```python
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from .connections import PostgresConfig, UnifiedQueryEngine
```

### Formatting

- **Line length**: 88 characters (ruff default)
- **Indentation**: 4 spaces
- **Quotes**: Double quotes for strings
- **Trailing commas**: Use in multi-line collections
- Run `uv run ruff format .` before committing

### Type Hints

Use type hints for function signatures. Prefer `Optional[T]` over `T | None` for consistency:

```python
def query(self, sql: str) -> pd.DataFrame:
    """Execute a SQL query and return results."""
    ...

def create_engine(
    postgres_host: str = "localhost",
    postgres_port: int = 5433,
) -> UnifiedQueryEngine:
    ...
```

### Docstrings

Use triple-quoted docstrings for modules, classes, and public functions:

```python
def function_name(arg: str) -> int:
    """
    Brief description of what the function does.

    Args:
        arg: Description of the argument

    Returns:
        Description of return value
    """
```

### Naming Conventions

- **Variables/functions**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `SCREAMING_SNAKE_CASE`
- **Private methods**: `_leading_underscore`

```python
NUM_CUSTOMERS = 10_000
BATCH_SIZE_ORDERS = 200_000

class QueryEngine:
    def _configure_s3(self) -> None:
        ...
```

### Error Handling

- Use specific exception types over bare `except:`
- Log warnings for non-fatal issues; raise for fatal ones
- Use `try/except` for external service calls (DB, S3)

```python
try:
    self.conn.execute(f"ATTACH '{pg.connection_string}' AS postgres_db...")
except Exception as e:
    print(f"Warning: Could not attach PostgreSQL: {e}")
```

### SQL in Python

- Use f-strings for SQL with dynamic table names
- Use triple-quoted strings for multi-line SQL
- Indent SQL consistently within Python strings

```python
result = qe.query(f"""
    SELECT 
        c.name,
        COUNT(o.order_id) as total_orders
    FROM postgres_db.public.customers c
    JOIN {qe.iceberg("orders")} o ON c.id = o.customer_id
    GROUP BY c.name
""")
```

### Dataclasses

Use `@dataclass` for configuration objects with sensible defaults:

```python
@dataclass
class PostgresConfig:
    host: str = "localhost"
    port: int = 5433
    database: str = "warehouse"

    @property
    def connection_string(self) -> str:
        return f"host={self.host} port={self.port}..."
```

## Architecture Notes

### Data Sources

| Source | Type | Location | Tables |
|--------|------|----------|--------|
| PostgreSQL | Transactional | `postgres_db.public.*` | customers, products |
| Analytics | Iceberg | `s3://analytics/` | orders, events |
| Inventory | Iceberg | `s3://inventory/` | suppliers, inventory_levels, shipments |
| Unity | Delta Lake | `s3://delta/` | product_reviews |

### Key Classes

- `UnifiedQueryEngine` (`src/connections.py`): Low-level DuckDB connection with extensions
- `QueryEngine` (`src/query_engine.py`): High-level API with Iceberg table helpers

### Querying Iceberg Tables

Use `iceberg_scan()` for Iceberg tables, not direct table references:

```python
# Correct
qe.query(f"SELECT * FROM {qe.iceberg('orders')} LIMIT 10")

# Also correct (raw)
qe.query("SELECT * FROM iceberg_scan('s3://analytics/default/orders')")
```

### Querying Delta Lake Tables

Use `qe.delta()` helper for Delta Lake tables:

```python
# Correct
qe.query(f"SELECT * FROM {qe.delta('product_reviews')} LIMIT 10")

# Also correct (raw)
qe.query("SELECT * FROM delta_scan('s3://delta/product_reviews') LIMIT 10")
```

## Dependencies

Managed via `uv` with `pyproject.toml`. Key dependencies:
- `duckdb>=1.2.0` - Query engine
- `pyiceberg[s3fs,pyarrow,sql-sqlite]>=0.9.0` - Iceberg table management
- `pandas>=2.0.0` - DataFrame results
- `pyarrow>=15.0.0` - Arrow columnar format
- `psycopg2-binary>=2.9.0` - PostgreSQL driver
- `deltalake>=1.0.0` - Delta Lake table management

## Common Tasks

```bash
# Verify infrastructure is running
docker compose ps

# Check MinIO buckets
docker compose exec minio mc ls local/

# Connect to PostgreSQL
docker compose exec postgres psql -U postgres -d warehouse
```
