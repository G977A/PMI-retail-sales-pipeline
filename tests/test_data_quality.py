"""
test_data_quality.py — Data quality assertions for the warehouse layer.

These are the same checks the Airflow DAG runs as its 'data_quality_checks' task,
reformulated as pytest tests so they can run in CI without needing Airflow.

Why duplicate them? Because CI should catch bugs on every commit, not only
when the DAG happens to run. If a teammate pushes a schema change that
breaks referential integrity, this test suite fails immediately on their PR —
before it's ever merged.
"""

import os

import psycopg2
import pytest


# -----------------------------------------------------------------------------
# DB connection helper
# -----------------------------------------------------------------------------
@pytest.fixture(scope="module")
def conn():
    """Opens one Postgres connection shared across all tests in the module."""
    connection = psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "retail"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
    )
    yield connection
    connection.close()


def scalar(conn, query):
    """Run a query that returns a single scalar value."""
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]


# -----------------------------------------------------------------------------
# Row count sanity
# -----------------------------------------------------------------------------
def test_fact_sales_has_rows(conn):
    """Fact table should never be empty after a successful load."""
    count = scalar(conn, "SELECT COUNT(*) FROM warehouse.fact_sales")
    assert count > 0, "fact_sales is empty — did the transformation run?"


def test_all_dimensions_populated(conn):
    """Every dimension referenced by fact_sales must have rows."""
    for table in ("dim_store", "dim_product", "dim_date"):
        count = scalar(conn, f"SELECT COUNT(*) FROM warehouse.{table}")
        assert count > 0, f"{table} is empty"


# -----------------------------------------------------------------------------
# Referential integrity
# -----------------------------------------------------------------------------
def test_no_null_store_keys(conn):
    """Every fact row must link to a valid store."""
    count = scalar(conn, """
        SELECT COUNT(*) FROM warehouse.fact_sales
        WHERE store_key IS NULL
    """)
    assert count == 0


def test_no_null_product_keys(conn):
    """Every fact row must link to a valid product."""
    count = scalar(conn, """
        SELECT COUNT(*) FROM warehouse.fact_sales
        WHERE product_key IS NULL
    """)
    assert count == 0


def test_no_orphan_store_keys(conn):
    """Every store_key in fact_sales must exist in dim_store."""
    count = scalar(conn, """
        SELECT COUNT(*) FROM warehouse.fact_sales fs
        LEFT JOIN warehouse.dim_store ds ON ds.store_key = fs.store_key
        WHERE ds.store_key IS NULL
    """)
    assert count == 0, "Found fact_sales rows pointing to non-existent stores"


def test_no_orphan_product_keys(conn):
    """Every product_key in fact_sales must exist in dim_product."""
    count = scalar(conn, """
        SELECT COUNT(*) FROM warehouse.fact_sales fs
        LEFT JOIN warehouse.dim_product dp ON dp.product_key = fs.product_key
        WHERE dp.product_key IS NULL
    """)
    assert count == 0, "Found fact_sales rows pointing to non-existent products"


# -----------------------------------------------------------------------------
# Business logic sanity
# -----------------------------------------------------------------------------
def test_no_negative_quantity(conn):
    """Quantities must be positive — negative = data corruption."""
    count = scalar(conn, """
        SELECT COUNT(*) FROM warehouse.fact_sales WHERE quantity <= 0
    """)
    assert count == 0


def test_no_negative_revenue(conn):
    """Total amount must be non-negative."""
    count = scalar(conn, """
        SELECT COUNT(*) FROM warehouse.fact_sales WHERE total_amount < 0
    """)
    assert count == 0


def test_transaction_ids_unique(conn):
    """No duplicate transaction IDs — would indicate a double load."""
    diff = scalar(conn, """
        SELECT COUNT(*) - COUNT(DISTINCT transaction_id)
        FROM warehouse.fact_sales
    """)
    assert diff == 0


# -----------------------------------------------------------------------------
# GDPR compliance
# -----------------------------------------------------------------------------
def test_pii_is_hashed(conn):
    """
    Warehouse must never contain plaintext PII.
    SHA-256 hashes are exactly 64 hex characters; anything else is suspicious.
    """
    bad = scalar(conn, """
        SELECT COUNT(*) FROM warehouse.dim_store
        WHERE owner_name_hash IS NOT NULL
          AND LENGTH(owner_name_hash) != 64
    """)
    assert bad == 0, "Found owner_name_hash values that are not 64-char SHA-256"

    bad = scalar(conn, """
        SELECT COUNT(*) FROM warehouse.dim_store
        WHERE owner_email_hash IS NOT NULL
          AND LENGTH(owner_email_hash) != 64
    """)
    assert bad == 0, "Found owner_email_hash values that are not 64-char SHA-256"


def test_warehouse_has_no_plaintext_email_columns(conn):
    """
    Schema-level check: warehouse tables must not have columns named 'email'
    or 'phone' (only hashed variants are allowed).
    Prevents accidental leakage if someone adds a column later.
    """
    count = scalar(conn, """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = 'warehouse'
          AND column_name IN ('owner_email', 'owner_phone', 'owner_name')
    """)
    assert count == 0, (
        "Warehouse schema contains plaintext PII columns. "
        "Only hashed versions (owner_*_hash) are allowed."
    ) 