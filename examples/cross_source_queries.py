#!/usr/bin/env python3
"""
Cross-Source Query Examples

This script demonstrates querying across PostgreSQL and Iceberg data sources
using DuckDB as a unified query engine.

Prerequisites:
1. Docker services running: docker compose up -d
2. Iceberg tables created: uv run python src/setup_iceberg.py

Usage:
    uv run python examples/cross_source_queries.py
"""

import sys

sys.path.insert(0, ".")

from src.query_engine import QueryEngine


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def main():
    print("Initializing Unified Query Engine...")
    qe = QueryEngine()

    # =========================================================================
    # 1. Explore Available Data Sources
    # =========================================================================
    print_section("1. Available Data Sources")

    sources = qe.get_available_sources()
    print("PostgreSQL Tables:")
    for table in sources["postgres"].get("tables", []):
        print(f"  - postgres_db.public.{table}")

    print("\nIceberg Tables:")
    for name, path in sources["iceberg"]["tables"].items():
        print(f"  - {name}: {path}")

    # =========================================================================
    # 2. Simple PostgreSQL Query
    # =========================================================================
    print_section("2. PostgreSQL: Customer Distribution by Region")

    result = qe.query("""
        SELECT 
            region,
            tier,
            COUNT(*) as customer_count
        FROM postgres_db.public.customers
        GROUP BY region, tier
        ORDER BY region, tier
    """)
    print(result.to_string(index=False))

    # =========================================================================
    # 3. Simple Iceberg Query
    # =========================================================================
    print_section("3. Iceberg: Order Summary by Status")

    result = qe.query(f"""
        SELECT 
            status,
            COUNT(*) as order_count,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_order_value
        FROM {qe.iceberg("orders")}
        GROUP BY status
        ORDER BY total_revenue DESC
    """)
    print(result.to_string(index=False))

    # =========================================================================
    # 4. Cross-Source JOIN: Customer Orders with Details
    # =========================================================================
    print_section("4. Cross-Source JOIN: Top Customers by Revenue")

    result = qe.query(f"""
        SELECT 
            c.name as customer_name,
            c.region,
            c.tier,
            COUNT(o.order_id) as total_orders,
            ROUND(SUM(o.total_amount), 2) as total_spent,
            ROUND(AVG(o.total_amount), 2) as avg_order_value
        FROM postgres_db.public.customers c
        JOIN {qe.iceberg("orders")} o ON c.id = o.customer_id
        GROUP BY c.id, c.name, c.region, c.tier
        ORDER BY total_spent DESC
        LIMIT 10
    """)
    print(result.to_string(index=False))

    # =========================================================================
    # 5. Three-Way JOIN: Customers + Orders + Products
    # =========================================================================
    print_section("5. Three-Way JOIN: Product Performance by Customer Tier")

    result = qe.query(f"""
        SELECT 
            p.category as product_category,
            c.tier as customer_tier,
            COUNT(DISTINCT c.id) as unique_customers,
            COUNT(o.order_id) as total_orders,
            ROUND(SUM(o.total_amount), 2) as total_revenue
        FROM postgres_db.public.customers c
        JOIN {qe.iceberg("orders")} o ON c.id = o.customer_id
        JOIN postgres_db.public.products p ON o.product_id = p.id
        GROUP BY p.category, c.tier
        ORDER BY p.category, total_revenue DESC
    """)
    print(result.to_string(index=False))

    # =========================================================================
    # 6. Analytical Query: Regional Performance Over Time
    # =========================================================================
    print_section("6. Analytical: Monthly Revenue by Region")

    result = qe.query(f"""
        SELECT 
            DATE_TRUNC('month', o.order_date) as month,
            c.region,
            COUNT(o.order_id) as orders,
            ROUND(SUM(o.total_amount), 2) as revenue
        FROM postgres_db.public.customers c
        JOIN {qe.iceberg("orders")} o ON c.id = o.customer_id
        WHERE o.status NOT IN ('cancelled')
        GROUP BY month, c.region
        ORDER BY month, revenue DESC
    """)
    print(result.to_string(index=False))

    # =========================================================================
    # 7. Event Analytics: Customer Engagement from Iceberg Events
    # =========================================================================
    print_section("7. Event Analytics: Customer Engagement Metrics")

    result = qe.query(f"""
        SELECT 
            c.name as customer_name,
            c.tier,
            COUNT(DISTINCT e.session_id) as sessions,
            COUNT(*) as total_events,
            SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
            ROUND(
                100.0 * SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) / 
                NULLIF(SUM(CASE WHEN e.event_type = 'checkout' THEN 1 ELSE 0 END), 0), 
                1
            ) as checkout_conversion_rate
        FROM postgres_db.public.customers c
        JOIN {qe.iceberg("events")} e ON c.id = e.customer_id
        GROUP BY c.id, c.name, c.tier
        HAVING COUNT(*) > 50
        ORDER BY total_events DESC
        LIMIT 10
    """)
    print(result.to_string(index=False))

    # =========================================================================
    # 8. Combining Orders and Events: Full Customer 360
    # =========================================================================
    print_section("8. Customer 360: Orders + Events Combined")

    result = qe.query(f"""
        WITH customer_orders AS (
            SELECT 
                customer_id,
                COUNT(*) as order_count,
                SUM(total_amount) as total_spent,
                MAX(order_date) as last_order_date
            FROM {qe.iceberg("orders")}
            WHERE status != 'cancelled'
            GROUP BY customer_id
        ),
        customer_events AS (
            SELECT
                customer_id,
                COUNT(*) as event_count,
                COUNT(DISTINCT session_id) as session_count,
                MAX(event_timestamp) as last_activity
            FROM {qe.iceberg("events")}
            GROUP BY customer_id
        )
        SELECT 
            c.name,
            c.region,
            c.tier,
            COALESCE(co.order_count, 0) as orders,
            ROUND(COALESCE(co.total_spent, 0), 2) as revenue,
            COALESCE(ce.session_count, 0) as sessions,
            COALESCE(ce.event_count, 0) as events,
            co.last_order_date,
            ce.last_activity
        FROM postgres_db.public.customers c
        LEFT JOIN customer_orders co ON c.id = co.customer_id
        LEFT JOIN customer_events ce ON c.id = ce.customer_id
        ORDER BY revenue DESC
        LIMIT 15
    """)
    print(result.to_string(index=False))

    # Cleanup
    qe.close()
    print("\n" + "=" * 60)
    print("  All examples completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
