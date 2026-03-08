# AGENTS.md

Guidelines for AI coding agents working in this repository.

## Project Overview

Unified Query Engine Prototype using DuckDB for federated queries across:
- **PostgreSQL** (transactional): customers, products -- accessed as `postgres_db.public.<table>`
- **Iceberg/Analytics** (`s3://analytics/`): orders, events -- accessed via `iceberg_scan()`
- **Iceberg/Inventory** (`s3://inventory/`): suppliers, inventory_levels, shipments
- **Delta Lake** (`s3://delta/`): product_reviews -- accessed via `delta_scan()`

Key classes: `UnifiedQueryEngine` (low-level DuckDB conn, `src/connections.py`) and
`QueryEngine` (high-level API with `iceberg()`/`delta()` helpers, `src/query_engine.py`).

## Build & Infrastructure

```bash
uv sync                    # Install dependencies (uses uv, not pip)
task setup                 # Full setup: uv sync + Docker + create Iceberg/Delta tables
task up / task down        # Start/stop Docker (PostgreSQL on :5433, MinIO on :9000)
task reset                 # Destroy all data and rebuild
```

## Test Commands

```bash
uv run pytest                                          # Run all tests
uv run pytest tests/test_query_engine.py               # Single test file
uv run pytest tests/test_query_engine.py::test_name    # Single test function
uv run pytest -k "pattern"                             # Tests matching a pattern
```

## Lint & Format

No explicit ruff config exists; all ruff defaults apply (88-char line length, E+F rules).

```bash
uv run ruff check .             # Lint
uv run ruff check . --fix       # Auto-fix lint issues
uv run ruff format .            # Format code
uv run ruff format . --check    # Check formatting without applying
```

Run both `ruff check` and `ruff format` before committing.

## Code Style

### Imports

Three groups separated by blank lines: (1) stdlib, (2) third-party, (3) local (relative).

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
- **Trailing commas**: Use in multi-line collections and function calls

### Type Hints

Use type hints on all function signatures. Prefer `Optional[T]` over `T | None`:

```python
def query(self, sql: str) -> pd.DataFrame: ...
def create_engine(host: str = "localhost", port: int = 5433) -> UnifiedQueryEngine: ...
```

Type-annotate instance variables in `__init__`:
```python
self._conn: Optional[duckdb.DuckDBPyConnection] = None
```

### Naming

- **Variables/functions**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `SCREAMING_SNAKE_CASE` (e.g., `NUM_CUSTOMERS = 10_000`)
- **Private methods/attrs**: `_leading_underscore`

### Docstrings

Google-style with `Args:`/`Returns:` sections. Use triple-quoted strings on modules,
classes, and public functions. Short one-liners are fine for simple methods.

```python
def execute(self, sql: str) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame.

    Args:
        sql: SQL query string

    Returns:
        Query results as a pandas DataFrame
    """
```

### Error Handling

- Use specific exception types; avoid bare `except:`
- Non-fatal failures (e.g., optional service unavailable): catch and `print()` a warning
- Fatal/invalid input: raise `ValueError` or appropriate exception
- External service calls (DB, S3): always wrap in `try/except`
- Use `try/finally` for cleanup (e.g., closing connections)

```python
try:
    self.conn.execute(f"ATTACH '{pg.connection_string}' AS postgres_db ...")
except Exception as e:
    print(f"Warning: Could not attach PostgreSQL: {e}")
```

### Dataclasses & SQL in Python

Use `@dataclass` with sensible defaults for configuration objects. Derive computed
values via `@property`:

```python
@dataclass
class PostgresConfig:
    host: str = "localhost"
    port: int = 5433

    @property
    def connection_string(self) -> str:
        return f"host={self.host} port={self.port} ..."
```

Use f-strings with triple quotes for multi-line SQL. Use `qe.iceberg("table")` /
`qe.delta("table")` helpers for scan expressions. Indent SQL body consistently.

```python
result = qe.query(f"""
    SELECT c.name, COUNT(o.order_id) AS total_orders
    FROM postgres_db.public.customers c
    JOIN {qe.iceberg("orders")} o ON c.id = o.customer_id
    GROUP BY c.name
""")
```

## Architecture

### Data Source Reference

| Source | Type | Query Pattern | Tables |
|--------|------|---------------|--------|
| PostgreSQL | Transactional | `postgres_db.public.<table>` | customers, products |
| Analytics | Iceberg | `qe.iceberg("<table>")` | orders, events |
| Inventory | Iceberg | `qe.iceberg("<table>")` | suppliers, inventory_levels, shipments |
| Delta | Delta Lake | `qe.delta("<table>")` | product_reviews |

### Key Files

| File | Purpose |
|------|---------|
| `src/connections.py` | `UnifiedQueryEngine` -- DuckDB connection, extensions, S3/PG setup |
| `src/query_engine.py` | `QueryEngine` -- high-level API, table registry, scan helpers |
| `src/setup_iceberg.py` | Creates Iceberg tables with synthetic data via PyIceberg |
| `src/setup_delta.py` | Creates Delta Lake tables with synthetic data via deltalake |
| `infrastructure/postgres/init.sql` | PostgreSQL schema + seed data (10k customers, 10 products) |
| `docker-compose.yml` | PostgreSQL (port 5433) + MinIO (port 9000) + bucket setup |
