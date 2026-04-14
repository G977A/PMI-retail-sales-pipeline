"""
gdpr_delete.py — Implements GDPR Article 17 "right to be forgotten".

Given a store_id, deletes all associated data from both the raw and
warehouse layers, and records the action in the audit log.

Usage:
    python scripts/gdpr_delete.py --store-id STR0042 --operator jane.doe
    python scripts/gdpr_delete.py --store-id STR0042 --operator jane.doe \
        --reason "Owner request #GDPR-2026-0147"
    python scripts/gdpr_delete.py --store-id STR0042 --operator jane.doe --dry-run

The --dry-run flag reports what WOULD be deleted without modifying data.
Always use it first in production to confirm the correct subject.

Exit codes:
    0 — success
    1 — subject not found (nothing to delete)
    2 — database error
    3 — invalid arguments
"""

import argparse
import os
import sys

import psycopg2


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
DB_CONFIG = {
    "host":     os.environ.get("PGHOST", "localhost"),
    "port":     int(os.environ.get("PGPORT", "5432")),
    "dbname":   os.environ.get("PGDATABASE", "retail"),
    "user":     os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}


# -----------------------------------------------------------------------------
# Core deletion logic
# -----------------------------------------------------------------------------
def count_matches(cur, store_id):
    """Preview the impact — how many rows will be affected in each table."""
    cur.execute("SELECT COUNT(*) FROM raw.stores WHERE store_id = %s", (store_id,))
    raw_stores = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM raw.sales WHERE store_id = %s", (store_id,))
    raw_sales = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM warehouse.dim_store WHERE store_id = %s",
        (store_id,),
    )
    wh_stores = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*) FROM warehouse.fact_sales fs
        JOIN warehouse.dim_store ds ON ds.store_key = fs.store_key
        WHERE ds.store_id = %s
        """,
        (store_id,),
    )
    wh_sales = cur.fetchone()[0]

    return {
        "raw_stores": raw_stores,
        "raw_sales":  raw_sales,
        "wh_stores":  wh_stores,
        "wh_sales":   wh_sales,
    }


def delete_subject(cur, store_id):
    """
    Execute the deletion. Order matters — fact_sales must go before dim_store
    because of the FK from fact_sales to dim_store.
    """
    # Warehouse: delete fact rows first, then the dimension row
    cur.execute(
        """
        DELETE FROM warehouse.fact_sales
        WHERE store_key IN (
            SELECT store_key FROM warehouse.dim_store WHERE store_id = %s
        )
        """,
        (store_id,),
    )

    cur.execute(
        "DELETE FROM warehouse.dim_store WHERE store_id = %s",
        (store_id,),
    )

    # Raw: delete sales first, then the store record
    cur.execute("DELETE FROM raw.sales WHERE store_id = %s", (store_id,))
    cur.execute("DELETE FROM raw.stores WHERE store_id = %s", (store_id,))


def write_audit_log(cur, store_id, operator, reason, counts):
    cur.execute(
        """
        INSERT INTO raw.gdpr_audit_log
            (subject_store_id, operator, reason,
             raw_stores_deleted, raw_sales_deleted,
             wh_stores_deleted, wh_sales_deleted)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            store_id,
            operator,
            reason,
            counts["raw_stores"],
            counts["raw_sales"],
            counts["wh_stores"],
            counts["wh_sales"],
        ),
    )


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="GDPR right-to-be-forgotten: delete a store's data."
    )
    parser.add_argument("--store-id", required=True,
                        help="Store identifier to delete (e.g., STR0042).")
    parser.add_argument("--operator", required=True,
                        help="Name or username of the person authorizing "
                             "the deletion (for audit log).")
    parser.add_argument("--reason", default=None,
                        help="Optional reason string — e.g., ticket number.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview affected rows without deleting anything.")
    args = parser.parse_args()

    print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        print(f"ERROR: Could not connect to database: {e}", file=sys.stderr)
        return 2

    try:
        with conn.cursor() as cur:
            counts = count_matches(cur, args.store_id)
            total = sum(counts.values())

            print(f"\nImpact preview for store_id = {args.store_id!r}:")
            print(f"  raw.stores:           {counts['raw_stores']:>6} rows")
            print(f"  raw.sales:            {counts['raw_sales']:>6} rows")
            print(f"  warehouse.dim_store:  {counts['wh_stores']:>6} rows")
            print(f"  warehouse.fact_sales: {counts['wh_sales']:>6} rows")
            print(f"  {'TOTAL':<20}  {total:>6} rows")

            if total == 0:
                print(f"\nNo data found for store_id {args.store_id!r}. Nothing to delete.")
                return 1

            if args.dry_run:
                print("\n[DRY RUN] No changes made.")
                return 0

            print(f"\nProceeding with deletion (operator={args.operator})...")
            delete_subject(cur, args.store_id)
            write_audit_log(cur, args.store_id, args.operator, args.reason, counts)
            conn.commit()
            print("✓ Deletion complete. Audit log entry written.")
            return 0

    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR: Database error during deletion: {e}", file=sys.stderr)
        return 2
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())