#!/usr/bin/env python3
"""
Cross-Source Query Examples

This script demonstrates querying across FOUR INDEPENDENT data sources
using DuckDB as a unified query engine:

1. PostgreSQL Database (transactional)
   - customers, products tables

2. Iceberg Analytics Catalog (s3://analytics/)
   - Separate catalog with its own metadata
   - orders, events tables

3. Iceberg Inventory Catalog (s3://inventory/)
   - Separate catalog with its own metadata
   - suppliers, inventory_levels, shipments tables

4. Delta Lake (s3://delta/)
   - Product reviews table (product_reviews)

This simulates a real federated data environment where an AI agent
would need to query across completely independent data sources.

Prerequisites:
    task setup

Usage:
    uv run python examples/cross_source_queries.py
"""

import sys
import time

sys.path.insert(0, ".")

from src.query_engine import QueryEngine


def print_section(title: str, source_info: str = ""):
    """Print a formatted section header."""
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    if source_info:
        print(f"  Source: {source_info}")
    print(f"{'=' * 70}\n")


def timed_query(qe: QueryEngine, sql: str) -> tuple:
    """Execute a query and return (result, elapsed_time_ms)."""
    start = time.perf_counter()
    result = qe.query(sql)
    elapsed_ms = (time.perf_counter() - start) * 1000
    return result, elapsed_ms


def print_result(result, elapsed_ms: float):
    """Print query result with timing."""
    print(result.to_string(index=False))
    print(f"\n  ⏱  Query time: {elapsed_ms:.1f} ms")


def main():
    print("Initializing Unified Query Engine...")
    print("Connecting to FOUR independent data sources:\n")
    print("  1. PostgreSQL        → Transactional data (customers, products)")
    print("  2. Iceberg Analytics → s3://analytics/ (orders, events)")
    print("  3. Iceberg Inventory → s3://inventory/ (suppliers, shipments)")
    print("  4. Delta Lake        → s3://delta/ (product_reviews)")

    qe = QueryEngine()
    total_query_time = 0.0

    # =========================================================================
    # 1. Explore Available Data Sources
    # =========================================================================
    print_section("1. Available Data Sources (4 Independent Sources)")

    sources = qe.get_available_sources()

    print("PostgreSQL Database:")
    for table in sources["postgres"].get("tables", []):
        print(f"  - postgres_db.public.{table}")

    print("\nIceberg Analytics Catalog (s3://analytics/):")
    for name, path in sources["iceberg_analytics"]["tables"].items():
        print(f"  - {name}: {path}")

    print("\nIceberg Inventory Catalog (s3://inventory/):")
    for name, path in sources["iceberg_inventory"]["tables"].items():
        print(f"  - {name}: {path}")

    print("\nDelta Lake (s3://delta/):")
    for name, path in sources["delta"]["tables"].items():
        print(f"  - {name}: {path}")

    # =========================================================================
    # 2. PostgreSQL Query
    # =========================================================================
    print_section("2. PostgreSQL: Customer Distribution", "PostgreSQL")

    result, elapsed = timed_query(
        qe,
        """
        SELECT 
            region,
            tier,
            COUNT(*) as customer_count
        FROM postgres_db.public.customers
        GROUP BY region, tier
        ORDER BY customer_count DESC
        LIMIT 10
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 3. Iceberg Analytics Catalog Query
    # =========================================================================
    print_section(
        "3. Iceberg Analytics: Order Summary (2M orders)",
        "Iceberg Catalog: s3://analytics/",
    )

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            status,
            COUNT(*) as order_count,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_order_value
        FROM {qe.iceberg("orders")}
        GROUP BY status
        ORDER BY total_revenue DESC
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 4. Iceberg Inventory Catalog Query
    # =========================================================================
    print_section(
        "4. Iceberg Inventory: Supplier Overview", "Iceberg Catalog: s3://inventory/"
    )

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            supplier_id,
            name,
            country,
            rating
        FROM {qe.iceberg("suppliers")}
        ORDER BY rating DESC
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 5. Cross-Source: PostgreSQL + Iceberg Analytics
    # =========================================================================
    print_section(
        "5. Cross-Source: Top Customers by Revenue", "PostgreSQL + Iceberg Analytics"
    )

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            c.name as customer_name,
            c.region,
            c.tier,
            COUNT(o.order_id) as total_orders,
            ROUND(SUM(o.total_amount), 2) as total_spent
        FROM postgres_db.public.customers c
        JOIN {qe.iceberg("orders")} o ON c.id = o.customer_id
        GROUP BY c.id, c.name, c.region, c.tier
        ORDER BY total_spent DESC
        LIMIT 10
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 6. Cross-Source: Two Iceberg Catalogs
    # =========================================================================
    print_section(
        "6. Cross-Catalog: Supplier Performance",
        "Iceberg Analytics + Iceberg Inventory (two separate catalogs)",
    )

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            sup.name as supplier_name,
            sup.country,
            sup.rating,
            COUNT(DISTINCT s.shipment_id) as total_shipments,
            SUM(s.quantity) as total_units_shipped,
            ROUND(100.0 * SUM(CASE WHEN s.status = 'delivered' THEN 1 ELSE 0 END) / COUNT(*), 1) as delivery_rate_pct
        FROM {qe.iceberg("suppliers")} sup
        JOIN {qe.iceberg("shipments")} s ON sup.supplier_id = s.supplier_id
        GROUP BY sup.supplier_id, sup.name, sup.country, sup.rating
        ORDER BY total_shipments DESC
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 7. Three-Way: PostgreSQL + Both Iceberg Catalogs
    # =========================================================================
    print_section(
        "7. Three-Source Join: Full Order Fulfillment",
        "PostgreSQL + Iceberg Analytics + Iceberg Inventory",
    )

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            c.region as customer_region,
            p.category as product_category,
            sup.country as supplier_country,
            COUNT(DISTINCT o.order_id) as orders,
            COUNT(DISTINCT s.shipment_id) as shipments,
            ROUND(SUM(o.total_amount), 2) as revenue
        FROM postgres_db.public.customers c
        JOIN {qe.iceberg("orders")} o ON c.id = o.customer_id
        JOIN postgres_db.public.products p ON o.product_id = p.id
        JOIN {qe.iceberg("shipments")} s ON o.order_id = s.order_id
        JOIN {qe.iceberg("suppliers")} sup ON s.supplier_id = sup.supplier_id
        GROUP BY c.region, p.category, sup.country
        ORDER BY revenue DESC
        LIMIT 15
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 8. Inventory Analysis
    # =========================================================================
    print_section("8. Low Stock Alert", "PostgreSQL + Iceberg Inventory")

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            p.name as product_name,
            p.category,
            i.warehouse_location,
            i.quantity_on_hand,
            i.reorder_point,
            CASE 
                WHEN i.quantity_on_hand < i.reorder_point THEN 'REORDER'
                WHEN i.quantity_on_hand < i.reorder_point * 1.5 THEN 'LOW'
                ELSE 'OK'
            END as stock_status
        FROM postgres_db.public.products p
        JOIN {qe.iceberg("inventory_levels")} i ON p.id = i.product_id
        WHERE i.quantity_on_hand < i.reorder_point
        ORDER BY (i.reorder_point - i.quantity_on_hand) DESC
        LIMIT 15
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 9. Time-Series Analysis
    # =========================================================================
    print_section("9. Monthly Revenue by Region and Supplier", "All Three Sources")

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            DATE_TRUNC('month', o.order_date) as month,
            c.region as customer_region,
            sup.country as supplier_country,
            COUNT(DISTINCT o.order_id) as orders,
            ROUND(SUM(o.total_amount), 2) as revenue
        FROM postgres_db.public.customers c
        JOIN {qe.iceberg("orders")} o ON c.id = o.customer_id
        JOIN {qe.iceberg("shipments")} s ON o.order_id = s.order_id
        JOIN {qe.iceberg("suppliers")} sup ON s.supplier_id = sup.supplier_id
        WHERE o.order_date >= '2024-01-01'
        GROUP BY month, c.region, sup.country
        ORDER BY month DESC, revenue DESC
        LIMIT 20
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 10. Customer 360 View
    # =========================================================================
    print_section(
        "10. Customer 360: Complete View",
        "PostgreSQL + Iceberg Analytics + Iceberg Inventory",
    )

    result, elapsed = timed_query(
        qe,
        f"""
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
                COUNT(DISTINCT session_id) as session_count
            FROM {qe.iceberg("events")}
            GROUP BY customer_id
        ),
        customer_shipments AS (
            SELECT
                o.customer_id,
                COUNT(DISTINCT s.shipment_id) as shipment_count,
                SUM(CASE WHEN s.status = 'delivered' THEN 1 ELSE 0 END) as delivered_count
            FROM {qe.iceberg("orders")} o
            JOIN {qe.iceberg("shipments")} s ON o.order_id = s.order_id
            GROUP BY o.customer_id
        )
        SELECT 
            c.name,
            c.region,
            c.tier,
            COALESCE(co.order_count, 0) as orders,
            ROUND(COALESCE(co.total_spent, 0), 2) as revenue,
            COALESCE(ce.session_count, 0) as sessions,
            COALESCE(cs.shipment_count, 0) as shipments,
            COALESCE(cs.delivered_count, 0) as delivered
        FROM postgres_db.public.customers c
        LEFT JOIN customer_orders co ON c.id = co.customer_id
        LEFT JOIN customer_events ce ON c.id = ce.customer_id
        LEFT JOIN customer_shipments cs ON c.id = cs.customer_id
        ORDER BY revenue DESC
        LIMIT 15
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 11. Supply Chain Analysis
    # =========================================================================
    print_section(
        "11. Supply Chain: Product Flow", "PostgreSQL + Both Iceberg Catalogs"
    )

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            p.name as product,
            p.category,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(o.quantity) as units_ordered,
            SUM(CASE WHEN s.status = 'delivered' THEN s.quantity ELSE 0 END) as units_delivered,
            ROUND(100.0 * SUM(CASE WHEN s.status = 'delivered' THEN s.quantity ELSE 0 END) / 
                  NULLIF(SUM(o.quantity), 0), 1) as fulfillment_rate_pct,
            COUNT(DISTINCT sup.supplier_id) as supplier_count
        FROM postgres_db.public.products p
        JOIN {qe.iceberg("orders")} o ON p.id = o.product_id
        LEFT JOIN {qe.iceberg("shipments")} s ON o.order_id = s.order_id
        LEFT JOIN {qe.iceberg("suppliers")} sup ON s.supplier_id = sup.supplier_id
        GROUP BY p.id, p.name, p.category
        ORDER BY total_orders DESC
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 12. Delta Lake Query
    # =========================================================================
    print_section(
        "12. Delta Lake: Product Reviews",
        "Delta Lake: s3://delta/",
    )

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            rating,
            sentiment,
            COUNT(*) as review_count,
            ROUND(AVG(helpful_votes), 1) as avg_helpful_votes
        FROM {qe.delta("product_reviews")}
        GROUP BY rating, sentiment
        ORDER BY rating DESC
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 13. Cross-Source: PostgreSQL + Delta Lake
    # =========================================================================
    print_section(
        "13. Cross-Source: Customer Reviews Analysis",
        "PostgreSQL + Delta Lake",
    )

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            c.region,
            c.tier,
            COUNT(r.review_id) as total_reviews,
            ROUND(AVG(r.rating), 2) as avg_rating,
            SUM(CASE WHEN r.sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
            SUM(CASE WHEN r.sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count
        FROM postgres_db.public.customers c
        JOIN {qe.delta("product_reviews")} r ON c.id = r.customer_id
        GROUP BY c.region, c.tier
        ORDER BY total_reviews DESC
        LIMIT 15
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 14. Four-Source Join: PostgreSQL + Iceberg + Delta Lake
    # =========================================================================
    print_section(
        "14. Four-Source Join: Complete Customer Satisfaction",
        "PostgreSQL + Iceberg Analytics + Iceberg Inventory + Delta Lake",
    )

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            c.region,
            p.category as product_category,
            sup.country as supplier_country,
            COUNT(DISTINCT o.order_id) as orders,
            COUNT(DISTINCT r.review_id) as reviews,
            ROUND(AVG(r.rating), 2) as avg_rating,
            ROUND(SUM(o.total_amount), 2) as revenue
        FROM postgres_db.public.customers c
        JOIN {qe.iceberg("orders")} o ON c.id = o.customer_id
        JOIN postgres_db.public.products p ON o.product_id = p.id
        JOIN {qe.iceberg("shipments")} s ON o.order_id = s.order_id
        JOIN {qe.iceberg("suppliers")} sup ON s.supplier_id = sup.supplier_id
        LEFT JOIN {qe.delta("product_reviews")} r ON o.order_id = r.order_id
        WHERE r.review_id IS NOT NULL
        GROUP BY c.region, p.category, sup.country
        ORDER BY avg_rating DESC, revenue DESC
        LIMIT 15
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # =========================================================================
    # 15. Product Performance with Reviews
    # =========================================================================
    print_section(
        "15. Product Performance: Sales vs Reviews",
        "PostgreSQL + Iceberg Analytics + Delta Lake",
    )

    result, elapsed = timed_query(
        qe,
        f"""
        SELECT 
            p.name as product,
            p.category,
            COUNT(DISTINCT o.order_id) as total_orders,
            ROUND(SUM(o.total_amount), 2) as total_revenue,
            COUNT(DISTINCT r.review_id) as review_count,
            ROUND(AVG(r.rating), 2) as avg_rating,
            ROUND(100.0 * SUM(CASE WHEN r.sentiment = 'positive' THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(r.review_id), 0), 1) as positive_pct
        FROM postgres_db.public.products p
        JOIN {qe.iceberg("orders")} o ON p.id = o.product_id
        LEFT JOIN {qe.delta("product_reviews")} r ON o.order_id = r.order_id
        GROUP BY p.id, p.name, p.category
        ORDER BY total_revenue DESC
    """,
    )
    print_result(result, elapsed)
    total_query_time += elapsed

    # Cleanup
    qe.close()

    print("\n" + "=" * 70)
    print("  All examples completed successfully!")
    print("=" * 70)
    print("\nSummary: Queried across 4 independent data sources:")
    print("  - PostgreSQL database (transactional)")
    print("  - Iceberg Analytics catalog (s3://analytics/)")
    print("  - Iceberg Inventory catalog (s3://inventory/)")
    print("  - Delta Lake (s3://delta/)")
    print(
        f"\n  Total query time: {total_query_time:.1f} ms ({total_query_time / 1000:.2f} s)"
    )
    print("\nThis demonstrates DuckDB's ability to federate queries across")
    print("completely separate data systems - ideal for AI agent integration.")


if __name__ == "__main__":
    main()
