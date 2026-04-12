"""
retail_sales_pipeline.py — The main ETL DAG.

This DAG orchestrates the daily retail sales pipeline:
    1. generate_daily_sales   — produce one new day of fake sales data
    2. transform_raw_to_warehouse — run the raw → warehouse SQL transformation
    3. data_quality_checks    — verify the load produced sane results
    4. log_summary            — print a final summary to the task logs

Schedule: every day at 02:00.

Failure handling:
    - Each task retries twice with a 5-minute delay before being marked failed.
    - If any task fails, all downstream tasks are skipped.
    - The DAG owner (you) gets an email on failure (configure SMTP separately).

Why this design:
    - Tasks are small and single-purpose. Easier to debug, easier to retry.
    - PythonOperator + a single SQL file keeps the logic in version control,
      not buried inside Airflow's metadata.
    - The data quality task uses Airflow's built-in failure mechanism
      (raise an exception) rather than a separate framework, keeping deps light.
"""

from datetime import datetime, timedelta
import logging
import os
import subprocess
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
WAREHOUSE_CONN = os.environ.get(
    "WAREHOUSE_CONN",
    "postgresql://postgres:postgres@postgres:5432/retail",
)
SQL_DIR = "/opt/airflow/sql"
GENERATOR_PATH = "/opt/airflow/data_generator/generate_sales.py"

DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # Set True + configure SMTP to enable
}

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Task 1: Generate one day of fake sales
# -----------------------------------------------------------------------------
def generate_daily_sales(**context):
    """
    Calls the existing generate_sales.py script with --days 1 --append.
    In a real pipeline this would be replaced by an extract from SAP, S3,
    an API, etc. The pattern is identical: pull source data into raw.*
    """
    # Override the DB host so the generator connects to the warehouse
    # container by service name, not localhost
    env = os.environ.copy()
    env["PGHOST"] = "postgres"

    result = subprocess.run(
        [sys.executable, GENERATOR_PATH, "--days", "1", "--append"],
        capture_output=True,
        text=True,
        env=env,
    )
    logger.info("Generator stdout:\n%s", result.stdout)
    if result.returncode != 0:
        logger.error("Generator stderr:\n%s", result.stderr)
        raise RuntimeError(f"Data generator failed with code {result.returncode}")


# -----------------------------------------------------------------------------
# Task 2: Run the raw → warehouse transformation SQL
# -----------------------------------------------------------------------------
def transform_raw_to_warehouse(**context):
    """
    Executes the transformation SQL file against the warehouse.
    Uses psycopg2 directly so failures bubble up as Python exceptions
    that Airflow can catch and retry.
    """
    import psycopg2

    sql_path = os.path.join(SQL_DIR, "transformations", "03_transform_raw_to_warehouse.sql")
    with open(sql_path, "r") as f:
        sql = f.read()

    conn = psycopg2.connect(WAREHOUSE_CONN)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("Transformation completed successfully")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Task 3: Data quality checks
# -----------------------------------------------------------------------------
def data_quality_checks(**context):
    """
    Runs a series of assertions against the warehouse.
    If any check fails, the task fails and downstream is blocked.

    These are the four most common DQ checks every fact table needs:
        - Row count is greater than zero (did anything load?)
        - No NULL surrogate keys (did all joins succeed?)
        - No negative measures (did the transformation logic work?)
        - All transaction_ids are unique (no double loads)
    """
    import psycopg2

    checks = [
        (
            "fact_sales has rows",
            "SELECT COUNT(*) FROM warehouse.fact_sales",
            lambda n: n > 0,
        ),
        (
            "no NULL store_keys in fact_sales",
            "SELECT COUNT(*) FROM warehouse.fact_sales WHERE store_key IS NULL",
            lambda n: n == 0,
        ),
        (
            "no NULL product_keys in fact_sales",
            "SELECT COUNT(*) FROM warehouse.fact_sales WHERE product_key IS NULL",
            lambda n: n == 0,
        ),
        (
            "no negative quantities",
            "SELECT COUNT(*) FROM warehouse.fact_sales WHERE quantity <= 0",
            lambda n: n == 0,
        ),
        (
            "no negative revenue",
            "SELECT COUNT(*) FROM warehouse.fact_sales WHERE total_amount < 0",
            lambda n: n == 0,
        ),
        (
            "transaction_ids are unique",
            """SELECT COUNT(*) - COUNT(DISTINCT transaction_id)
               FROM warehouse.fact_sales""",
            lambda n: n == 0,
        ),
    ]

    conn = psycopg2.connect(WAREHOUSE_CONN)
    failures = []
    try:
        with conn.cursor() as cur:
            for name, query, predicate in checks:
                cur.execute(query)
                result = cur.fetchone()[0]
                passed = predicate(result)
                status = "PASS" if passed else "FAIL"
                logger.info("  [%s] %s (got %s)", status, name, result)
                if not passed:
                    failures.append(f"{name} (got {result})")
    finally:
        conn.close()

    if failures:
        raise ValueError(f"Data quality checks failed: {failures}")
    logger.info("All %d data quality checks passed", len(checks))


# -----------------------------------------------------------------------------
# Task 4: Log a summary
# -----------------------------------------------------------------------------
def log_summary(**context):
    """Pretty-print a final summary so you can see the load result at a glance."""
    import psycopg2

    queries = {
        "fact_sales row count":   "SELECT COUNT(*) FROM warehouse.fact_sales",
        "dim_store row count":    "SELECT COUNT(*) FROM warehouse.dim_store",
        "dim_product row count":  "SELECT COUNT(*) FROM warehouse.dim_product",
        "dim_date row count":     "SELECT COUNT(*) FROM warehouse.dim_date",
        "total revenue (all time)":
            "SELECT COALESCE(SUM(total_amount), 0) FROM warehouse.fact_sales",
        "latest sale date":
            "SELECT MAX(date_key) FROM warehouse.fact_sales",
    }

    conn = psycopg2.connect(WAREHOUSE_CONN)
    try:
        with conn.cursor() as cur:
            logger.info("=" * 60)
            logger.info("PIPELINE RUN SUMMARY")
            logger.info("=" * 60)
            for label, query in queries.items():
                cur.execute(query)
                value = cur.fetchone()[0]
                logger.info("  %-30s %s", label + ":", value)
            logger.info("=" * 60)
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# DAG definition
# -----------------------------------------------------------------------------
with DAG(
    dag_id="retail_sales_pipeline",
    description="Daily ETL: generate sales → transform → validate → summarize",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",  # 02:00 daily
    catchup=False,
    max_active_runs=1,
    tags=["retail", "etl", "warehouse"],
) as dag:

    t1_generate = PythonOperator(
        task_id="generate_daily_sales",
        python_callable=generate_daily_sales,
    )

    t2_transform = PythonOperator(
        task_id="transform_raw_to_warehouse",
        python_callable=transform_raw_to_warehouse,
    )

    t3_quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks,
    )

    t4_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
    )

    # Dependency chain: each task runs only if the previous one succeeded
    t1_generate >> t2_transform >> t3_quality >> t4_summary 