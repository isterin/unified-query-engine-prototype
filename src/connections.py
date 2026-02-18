"""
DuckDB connection management with multi-source support.

This module provides a unified interface for connecting DuckDB to multiple
data sources including PostgreSQL and Iceberg tables stored in S3-compatible storage.
"""

import duckdb
from dataclasses import dataclass
from typing import Optional
import pandas as pd


@dataclass
class PostgresConfig:
    """Configuration for PostgreSQL connection."""

    host: str = "localhost"
    port: int = 5433
    database: str = "warehouse"
    user: str = "postgres"
    password: str = "postgres"

    @property
    def connection_string(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"


@dataclass
class S3Config:
    """Configuration for S3-compatible storage (MinIO)."""

    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    use_ssl: bool = False
    region: str = "us-east-1"
    url_style: str = "path"  # path style for MinIO


class UnifiedQueryEngine:
    """
    Unified query engine that connects DuckDB to multiple data sources.

    Supports:
    - PostgreSQL via the postgres extension
    - Iceberg tables via the iceberg extension (reading from S3/MinIO)
    - Local files (Parquet, CSV, etc.)

    Example:
        engine = UnifiedQueryEngine()
        engine.setup()

        # Query across sources
        result = engine.execute('''
            SELECT c.name, SUM(o.amount) as total
            FROM postgres_db.public.customers c
            JOIN iceberg_scan('s3://warehouse/orders') o ON c.id = o.customer_id
            GROUP BY c.name
        ''')
    """

    def __init__(
        self,
        postgres_config: Optional[PostgresConfig] = None,
        s3_config: Optional[S3Config] = None,
        database_path: str = ":memory:",
    ):
        self.postgres_config = postgres_config or PostgresConfig()
        self.s3_config = s3_config or S3Config()
        self.database_path = database_path
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create the DuckDB connection."""
        if self._conn is None:
            self._conn = duckdb.connect(self.database_path)
        return self._conn

    def setup(self) -> "UnifiedQueryEngine":
        """
        Initialize the query engine with all extensions and connections.

        Returns self for method chaining.
        """
        self._install_extensions()
        self._configure_s3()
        self._attach_postgres()
        return self

    def _install_extensions(self) -> None:
        """Install and load required DuckDB extensions."""
        extensions = ["postgres", "iceberg", "httpfs"]

        for ext in extensions:
            self.conn.execute(f"INSTALL {ext};")
            self.conn.execute(f"LOAD {ext};")

        # Enable version guessing for Iceberg tables (safe for local dev)
        # This is needed because PyIceberg doesn't create version-hint.txt files
        self.conn.execute("SET unsafe_enable_version_guessing = true;")

        print(f"Loaded extensions: {', '.join(extensions)}")

    def _configure_s3(self) -> None:
        """Configure S3 credentials for accessing MinIO/Iceberg storage."""
        s3 = self.s3_config

        # Create a secret for S3 access
        self.conn.execute(f"""
            CREATE OR REPLACE SECRET minio_secret (
                TYPE s3,
                KEY_ID '{s3.access_key}',
                SECRET '{s3.secret_key}',
                ENDPOINT '{s3.endpoint}',
                URL_STYLE '{s3.url_style}',
                USE_SSL {str(s3.use_ssl).lower()},
                REGION '{s3.region}'
            );
        """)

        print(f"Configured S3 access to {s3.endpoint}")

    def _attach_postgres(self) -> None:
        """Attach PostgreSQL database."""
        pg = self.postgres_config

        try:
            self.conn.execute(f"""
                ATTACH '{pg.connection_string}' AS postgres_db (TYPE postgres, READ_ONLY);
            """)
            print(f"Attached PostgreSQL database: {pg.database}")
        except Exception as e:
            print(f"Warning: Could not attach PostgreSQL: {e}")
            print(
                "PostgreSQL queries will not be available until the database is running."
            )

    def execute(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.

        Args:
            sql: SQL query string

        Returns:
            Query results as a pandas DataFrame
        """
        return self.conn.execute(sql).fetchdf()

    def execute_raw(self, sql: str) -> duckdb.DuckDBPyRelation:
        """
        Execute a SQL query and return the raw DuckDB relation.

        Useful for lazy evaluation or further transformations.
        """
        return self.conn.execute(sql)

    def get_postgres_tables(self) -> pd.DataFrame:
        """List all tables in the attached PostgreSQL database."""
        return self.execute("""
            SELECT table_schema, table_name 
            FROM postgres_db.information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)

    def describe_table(self, table_path: str) -> pd.DataFrame:
        """
        Describe the schema of a table.

        Args:
            table_path: Full path like 'postgres_db.public.customers'
        """
        return self.execute(f"DESCRIBE {table_path};")

    def iceberg_scan(self, table_path: str, **kwargs) -> pd.DataFrame:
        """
        Scan an Iceberg table from S3 storage.

        Args:
            table_path: S3 path to the Iceberg table (e.g., 's3://warehouse/orders')
            **kwargs: Additional options for iceberg_scan

        Returns:
            Table contents as a DataFrame
        """
        options = ", ".join(f"{k} = {v}" for k, v in kwargs.items())
        options_str = f", {options}" if options else ""

        return self.execute(f"""
            SELECT * FROM iceberg_scan('{table_path}'{options_str});
        """)

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def create_engine(
    postgres_host: str = "localhost",
    postgres_port: int = 5433,
    minio_endpoint: str = "localhost:9000",
) -> UnifiedQueryEngine:
    """
    Factory function to create a configured UnifiedQueryEngine.

    Args:
        postgres_host: PostgreSQL host
        postgres_port: PostgreSQL port
        minio_endpoint: MinIO S3 endpoint

    Returns:
        Configured and initialized UnifiedQueryEngine
    """
    pg_config = PostgresConfig(host=postgres_host, port=postgres_port)
    s3_config = S3Config(endpoint=minio_endpoint)

    engine = UnifiedQueryEngine(postgres_config=pg_config, s3_config=s3_config)

    return engine.setup()


if __name__ == "__main__":
    # Quick test of the connection setup
    print("Creating unified query engine...")
    engine = create_engine()

    print("\nPostgreSQL tables:")
    try:
        print(engine.get_postgres_tables())
    except Exception as e:
        print(f"Could not list tables: {e}")

    engine.close()
