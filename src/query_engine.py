"""
Query engine module providing high-level interface for cross-source queries.

This module wraps the connection layer and provides convenient methods
for executing queries across PostgreSQL and Iceberg data sources.
"""

from typing import Any, Dict, List, Optional

import pandas as pd

from .connections import PostgresConfig, S3Config, UnifiedQueryEngine, create_engine


class QueryEngine:
    """
    High-level query interface for cross-source data access.

    This class provides a simplified interface for:
    - Executing SQL queries across multiple data sources
    - Getting schema information for available tables
    - Managing Iceberg table paths

    Example:
        qe = QueryEngine()

        # Simple query
        customers = qe.query("SELECT * FROM postgres_db.public.customers")

        # Cross-source join
        results = qe.query('''
            SELECT c.name, c.region, SUM(o.total_amount) as revenue
            FROM postgres_db.public.customers c
            JOIN iceberg_scan('s3://warehouse/analytics/orders') o
                ON c.id = o.customer_id
            GROUP BY c.name, c.region
        ''')
    """

    # Default Iceberg table paths (set after running setup_iceberg.py)
    ICEBERG_TABLES = {
        "orders": "s3://warehouse/analytics/orders",
        "events": "s3://warehouse/analytics/events",
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
            "iceberg": {
                "type": "Iceberg (S3/MinIO)",
                "tables": dict(self.ICEBERG_TABLES),
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
            source: 'postgres' or 'iceberg'
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
        else:
            raise ValueError(f"Unknown source: {source}")

    def preview(self, source: str, table: str, limit: int = 10) -> pd.DataFrame:
        """
        Preview data from a table.

        Args:
            source: 'postgres' or 'iceberg'
            table: Table name
            limit: Number of rows to return

        Returns:
            Sample rows as a DataFrame
        """
        if source == "postgres":
            return self.query(f"SELECT * FROM postgres_db.public.{table} LIMIT {limit}")
        elif source == "iceberg":
            return self.query(f"SELECT * FROM {self.iceberg(table)} LIMIT {limit}")
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
