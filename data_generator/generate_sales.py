"""
generate_sales.py — Fake retail sales data generator.

Produces realistic-looking stores, products, and daily sales transactions
that mimic what a tobacco distributor's data sources would emit.

Usage:
    python data_generator/generate_sales.py --days 90
    python data_generator/generate_sales.py --days 1 --append   # daily incremental

The script writes directly into the `raw` schema in Postgres.
"""

import argparse
import random
from datetime import date, timedelta
from decimal import Decimal

import psycopg2
import os

from faker import Faker

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
# Host comes from PGHOST env var so this works both locally (localhost)
# and from inside Docker containers (postgres service name).
DB_CONFIG = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "retail"),
    "user": os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}

NUM_STORES = 50
NUM_PRODUCTS = 30

STORE_TYPES = ["kiosk", "supermarket", "gas_station", "convenience"]
REGIONS = ["Tbilisi", "Kutaisi", "Batumi", "Rustavi", "Gori", "Zugdidi"]
BRANDS = ["Marlboro", "L&M", "Parliament", "Chesterfield", "Bond Street"]
CATEGORIES = ["Premium", "Mid-Price", "Value", "Heated Tobacco"]

fake = Faker()
Faker.seed(42)
random.seed(42)


# -----------------------------------------------------------------------------
# Generators
# -----------------------------------------------------------------------------
def generate_stores(n=NUM_STORES):
    """Create N fake stores. Each has owner PII that will be pseudonymized later."""
    stores = []
    for i in range(1, n + 1):
        stores.append((
            f"STR{i:04d}",
            f"{fake.company()} {random.choice(['Market', 'Shop', 'Store'])}",
            random.choice(STORE_TYPES),
            random.choice(REGIONS),
            random.choice(REGIONS),
            fake.name(),
            fake.email(),
            fake.phone_number(),
            fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
        ))
    return stores


def generate_products(n=NUM_PRODUCTS):
    """Create N fake SKUs across brands and categories."""
    products = []
    for i in range(1, n + 1):
        brand = random.choice(BRANDS)
        products.append((
            f"PRD{i:04d}",
            f"SKU-{brand[:3].upper()}-{i:04d}",
            brand,
            f"{brand} {random.choice(['Red', 'Blue', 'Gold', 'Silver', 'Touch'])}",
            random.choice(CATEGORIES),
            str(round(random.uniform(3.50, 12.00), 2)),
        ))
    return products


def generate_sales_for_day(day: date, stores, products, txn_counter):
    """
    Generate transactions for a single day.
    Each store does 5–25 transactions per day.
    """
    sales = []
    for store in stores:
        store_id = store[0]
        num_txns = random.randint(5, 25)
        for _ in range(num_txns):
            product = random.choice(products)
            product_id = product[0]
            unit_price = Decimal(product[5])
            quantity = random.randint(1, 5)
            total = unit_price * quantity

            txn_counter[0] += 1
            sales.append((
                f"TXN{txn_counter[0]:010d}",
                store_id,
                product_id,
                day.isoformat(),
                str(quantity),
                str(unit_price),
                str(total),
            ))
    return sales


# -----------------------------------------------------------------------------
# DB writers
# -----------------------------------------------------------------------------
def write_stores(conn, stores):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE raw.stores;")
        cur.executemany(
            """
            INSERT INTO raw.stores
            (store_id, store_name, store_type, city, region,
             owner_name, owner_email, owner_phone, opened_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            stores,
        )
    conn.commit()
    print(f"  ✓ Inserted {len(stores)} stores")


def write_products(conn, products):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE raw.products;")
        cur.executemany(
            """
            INSERT INTO raw.products
            (product_id, sku, brand, product_name, category, unit_price)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            products,
        )
    conn.commit()
    print(f"  ✓ Inserted {len(products)} products")


def write_sales(conn, sales, append=False):
    with conn.cursor() as cur:
        if not append:
            cur.execute("TRUNCATE TABLE raw.sales;")
        cur.executemany(
            """
            INSERT INTO raw.sales
            (transaction_id, store_id, product_id, sale_date,
             quantity, unit_price, total_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            sales,
        )
    conn.commit()
    print(f"  ✓ Inserted {len(sales)} sales transactions")


def table_is_empty(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        return cur.fetchone()[0] == 0


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate fake retail sales data.")
    parser.add_argument("--days", type=int, default=90,
                        help="Number of days of history to generate.")
    parser.add_argument("--append", action="store_true",
                        help="Append to existing sales instead of truncating.")
    args = parser.parse_args()

    print(f"Connecting to Postgres at {DB_CONFIG['host']}:{DB_CONFIG['port']}...")
    conn = psycopg2.connect(**DB_CONFIG)

    print(f"\nGenerating {args.days} day(s) of sales data...")
    stores = generate_stores()
    products = generate_products()

    # In full-refresh mode, always rewrite master data.
    # In append mode, seed master data only if the tables are empty.
    if not args.append or table_is_empty(conn, "raw.stores"):
        write_stores(conn, stores)

    if not args.append or table_is_empty(conn, "raw.products"):
        write_products(conn, products)

    all_sales = []
    txn_counter = [0]

    # If appending, resume the txn counter from the highest existing ID
    # so we don't collide with existing transaction_ids.
    if args.append:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(CAST(SUBSTRING(transaction_id FROM 4) AS BIGINT)) "
                "FROM raw.sales WHERE transaction_id LIKE 'TXN%'"
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                txn_counter[0] = int(row[0])

    end_date = date.today()
    start_date = end_date - timedelta(days=args.days - 1)

    current = start_date
    while current <= end_date:
        day_sales = generate_sales_for_day(current, stores, products, txn_counter)
        all_sales.extend(day_sales)
        current += timedelta(days=1)

    write_sales(conn, all_sales, append=args.append)
    conn.close()

    print(f"\nDone. Generated {len(all_sales)} transactions across "
          f"{args.days} day(s) for {len(stores)} stores and {len(products)} products.")


if __name__ == "__main__":
    main()