"""
Query engine module providing high-level interface for cross-source queries.

This module wraps the connection layer and provides convenient methods
for executing queries across PostgreSQL, Iceberg, and Delta Lake data sources.
"""

from typing import Any, Dict, Optional

import pandas as pd

from .connections import UnifiedQueryEngine, create_engine


class QueryEngine:
    """
    High-level query interface for cross-source data access.

    This class provides a simplified interface for:
    - Executing SQL queries across multiple data sources
    - Getting schema information for available tables
    - Managing Iceberg and Delta Lake table paths

    Data Sources:
    - PostgreSQL: Transactional data (customers, products)
    - Iceberg Analytics Catalog: Customer analytics (orders, events)
    - Iceberg Inventory Catalog: Supply chain data (suppliers, shipments, inventory)
    - Delta Lake: Product reviews (product_reviews)

    Example:
        qe = QueryEngine()

        # Simple query
        customers = qe.query("SELECT * FROM postgres_db.public.customers")

        # Cross-source join (PostgreSQL + Iceberg Analytics)
        results = qe.query(f'''
            SELECT c.name, c.region, SUM(o.total_amount) as revenue
            FROM postgres_db.public.customers c
            JOIN {qe.iceberg('orders')} o ON c.id = o.customer_id
            GROUP BY c.name, c.region
        ''')

        # Four-source join (PostgreSQL + Iceberg + Delta Lake)
        results = qe.query(f'''
            SELECT c.name, r.rating, r.review_text
            FROM postgres_db.public.customers c
            JOIN {qe.iceberg('orders')} o ON c.id = o.customer_id
            JOIN {qe.delta('product_reviews')} r ON o.order_id = r.order_id
        ''')
    """

    # Iceberg table paths organized by catalog
    # These are TWO SEPARATE Iceberg catalogs with independent metadata and storage
    ICEBERG_CATALOGS = {
        "analytics": {
            "description": "Customer analytics data lake",
            "bucket": "s3://analytics",
            "tables": {
                "orders": "s3://analytics/default/orders",
                "events": "s3://analytics/default/events",
            },
        },
        "inventory": {
            "description": "Supply chain data lake",
            "bucket": "s3://inventory",
            "tables": {
                "suppliers": "s3://inventory/default/suppliers",
                "inventory_levels": "s3://inventory/default/inventory_levels",
                "shipments": "s3://inventory/default/shipments",
            },
        },
    }

    # Flat lookup for convenience
    ICEBERG_TABLES = {
        # Analytics catalog
        "orders": "s3://analytics/default/orders",
        "events": "s3://analytics/default/events",
        # Inventory catalog
        "suppliers": "s3://inventory/default/suppliers",
        "inventory_levels": "s3://inventory/default/inventory_levels",
        "shipments": "s3://inventory/default/shipments",
    }

    # Delta Lake tables
    DELTA_TABLES = {
        "product_reviews": "s3://delta/product_reviews",
    }

    def __init__(
        self,
        postgres_host: str = "localhost",
        postgres_port: int = 5433,
        minio_endpoint: str = "localhost:9000",
    ):
        self._engine: Optional[UnifiedQueryEngine] = None
        self._postgres_host = postgres_host
        self._postgres_port = postgres_port
        self._minio_endpoint = minio_endpoint

    @property
    def engine(self) -> UnifiedQueryEngine:
        """Lazy initialization of the query engine."""
        if self._engine is None:
            self._engine = create_engine(
                postgres_host=self._postgres_host,
                postgres_port=self._postgres_port,
                minio_endpoint=self._minio_endpoint,
            )
        return self._engine

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.

        Supports queries across:
        - PostgreSQL tables: postgres_db.public.<table_name>
        - Iceberg tables: iceberg_scan('s3://warehouse/...')
        - DuckDB functions and operations

        Args:
            sql: SQL query string

        Returns:
            Query results as a pandas DataFrame
        """
        return self.engine.execute(sql)

    def iceberg(self, table_name: str) -> str:
        """
        Get the iceberg_scan SQL fragment for a registered Iceberg table.

        Args:
            table_name: Short name like 'orders' or 'events'

        Returns:
            SQL fragment like "iceberg_scan('s3://warehouse/analytics/orders')"
        """
        if table_name not in self.ICEBERG_TABLES:
            raise ValueError(
                f"Unknown Iceberg table: {table_name}. "
                f"Available tables: {list(self.ICEBERG_TABLES.keys())}"
            )
        return f"iceberg_scan('{self.ICEBERG_TABLES[table_name]}')"

    def delta(self, table_name: str) -> str:
        """
        Get the delta_scan SQL fragment for a Delta Lake table.

        Args:
            table_name: Short name like 'product_reviews'

        Returns:
            SQL fragment like "delta_scan('s3://delta/product_reviews')"
        """
        if table_name not in self.DELTA_TABLES:
            raise ValueError(
                f"Unknown Delta Lake table: {table_name}. "
                f"Available tables: {list(self.DELTA_TABLES.keys())}"
            )
        return f"delta_scan('{self.DELTA_TABLES[table_name]}')"

    # Backward compatibility alias
    unity = delta

    def get_available_sources(self) -> Dict[str, Any]:
        """
        Return information about all available data sources.

        Returns:
            Dictionary with source information
        """
        sources = {
            "postgres": {
                "type": "PostgreSQL",
                "prefix": "postgres_db.public",
                "tables": [],
            },
            "iceberg_analytics": {
                "type": "Iceberg Catalog (Analytics)",
                "bucket": "s3://analytics",
                "tables": self.ICEBERG_CATALOGS["analytics"]["tables"],
            },
            "iceberg_inventory": {
                "type": "Iceberg Catalog (Inventory)",
                "bucket": "s3://inventory",
                "tables": self.ICEBERG_CATALOGS["inventory"]["tables"],
            },
            "delta": {
                "type": "Delta Lake",
                "bucket": "s3://delta",
                "tables": self.DELTA_TABLES,
            },
        }

        # Try to get PostgreSQL tables
        try:
            pg_tables = self.engine.get_postgres_tables()
            sources["postgres"]["tables"] = pg_tables["table_name"].tolist()
        except Exception as e:
            sources["postgres"]["error"] = str(e)

        return sources

    def describe(self, source: str, table: str) -> pd.DataFrame:
        """
        Describe the schema of a table.

        Args:
            source: 'postgres', 'iceberg', or 'unity'
            table: Table name

        Returns:
            Schema information as a DataFrame
        """
        if source == "postgres":
            return self.engine.describe_table(f"postgres_db.public.{table}")
        elif source == "iceberg":
            if table not in self.ICEBERG_TABLES:
                raise ValueError(f"Unknown Iceberg table: {table}")
            return self.query(f"DESCRIBE SELECT * FROM {self.iceberg(table)}")
        elif source == "delta":
            if table not in self.DELTA_TABLES:
                raise ValueError(f"Unknown Delta Lake table: {table}")
            return self.query(f"DESCRIBE SELECT * FROM {self.delta(table)}")
        else:
            raise ValueError(f"Unknown source: {source}")

    def preview(self, source: str, table: str, limit: int = 10) -> pd.DataFrame:
        """
        Preview data from a table.

        Args:
            source: 'postgres', 'iceberg', or 'unity'
            table: Table name
            limit: Number of rows to return

        Returns:
            Sample rows as a DataFrame
        """
        if source == "postgres":
            return self.query(f"SELECT * FROM postgres_db.public.{table} LIMIT {limit}")
        elif source == "iceberg":
            return self.query(f"SELECT * FROM {self.iceberg(table)} LIMIT {limit}")
        elif source == "delta":
            return self.query(f"SELECT * FROM {self.delta(table)} LIMIT {limit}")
        else:
            raise ValueError(f"Unknown source: {source}")

    def close(self):
        """Close the underlying connection."""
        if self._engine is not None:
            self._engine.close()
            self._engine = None


# Convenience function for quick queries
def quick_query(sql: str) -> pd.DataFrame:
    """
    Execute a one-off query across data sources.

    Creates a temporary QueryEngine, executes the query, and closes.
    For multiple queries, use QueryEngine directly.
    """
    qe = QueryEngine()
    try:
        return qe.query(sql)
    finally:
        qe.close()
