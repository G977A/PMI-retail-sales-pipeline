# Retail Sales Pipeline

![CI](https://github.com/G977A/PMI-retail-sales-pipeline/actions/workflows/ci.yml/badge.svg)

End-to-end data platform simulating a tobacco distributor's retail sales
operations. Built to demonstrate production-grade data engineering:
orchestrated ETL, dimensional warehousing, data quality, CI/CD, and
GDPR-compliant data handling.

---

## At a Glance

- **Orchestration:** Apache Airflow DAG running daily with retries + monitoring
- **Warehouse:** PostgreSQL star schema (fact + 3 dimensions), indexed for analytics
- **Data Quality:** 11 automated assertions run on every pipeline execution
- **CI/CD:** GitHub Actions runs the full pipeline on every push
- **GDPR:** PII pseudonymized at the layer boundary + audited right-to-be-forgotten

---

## Architecture

```
┌──────────────────┐
│  Data Generator  │  Python + Faker — produces daily fake sales data
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   raw schema     │  Landing zone — data as-is from sources (TEXT columns)
│  (Postgres)      │  Contains plaintext PII for operational use only
└────────┬─────────┘
         │
         ▼
┌──────────────────┐       ┌──────────────────┐
│  Airflow DAG     │──────▶│   SQL transform  │
│  (4 tasks, DAG)  │       │  (idempotent)    │
└────────┬─────────┘       └──────────────────┘
         │
         ▼
┌──────────────────┐
│warehouse schema  │  Star schema — fact_sales + dim_store/product/date
│ (Postgres)       │  PII replaced with SHA-256 hashes (GDPR)
└──────────────────┘

Wrapped around everything:
• GitHub Actions CI — lint + integration test on every push
• Data quality suite — 11 pytest assertions covering RI, business logic, GDPR
• Audit log — every PII deletion recorded in raw.gdpr_audit_log
```

## Data Model

The warehouse is a classic Kimball star schema.

**Grain of `fact_sales`:** one row per individual retail transaction.

| Table | Type | Rows (90 days) | Purpose |
|---|---|---|---|
| `fact_sales` | Fact | ~67,000 | Core transactional data |
| `dim_store` | Dimension | 50 | Store attributes (PII pseudonymized) |
| `dim_product` | Dimension | 30 | SKU catalog |
| `dim_date` | Dimension | ~120 | Pre-built calendar with fiscal attributes |

Surrogate keys (`BIGSERIAL`) on every dimension. Foreign keys enforced on
the fact table. Composite indexes chosen for the three most common
retail analytics query patterns: by store×date, by product×date, and
across all stores by date.

## Pipeline

The Airflow DAG `retail_sales_pipeline` runs daily at 02:00.

![Airflow Graph](docs/airflow_graph.png)

| Task | Does | On failure |
|---|---|---|
| `generate_daily_sales` | Ingests one day of source data | Retry 2× with 5min delay |
| `transform_raw_to_warehouse` | Applies type casting, PII hashing, surrogate key lookup | Rollback — transaction-wrapped |
| `data_quality_checks` | 6 in-pipeline assertions | Block downstream tasks |
| `log_summary` | Records row counts + revenue for observability | Retry |

All four tasks chain sequentially. A failure at any stage blocks the next.
The transformation is idempotent — a retried run produces identical output.

## Data Quality

Every run is validated against 11 assertions (`tests/test_data_quality.py`):

**Structural integrity**
- `fact_sales` has rows after every load
- All three dimensions are populated
- No NULL surrogate keys in the fact table
- No orphan foreign keys (all `store_key`s exist in `dim_store`, etc.)

**Business logic**
- No negative quantities
- No negative revenue
- No duplicate `transaction_id`s (catches double loads)

**GDPR compliance**
- PII hashes are all valid SHA-256 (64 hex chars)
- Warehouse schema contains zero plaintext PII columns

Failing any check fails the CI build and blocks merge.

## GDPR & Data Governance

See [`GDPR.md`](./GDPR.md) for the full data governance policy.

**Short version:**

- `owner_name`, `owner_email` exist in the `raw` layer only (operational use)
- Both are SHA-256 hashed before crossing into the `warehouse` layer
- `owner_phone` is dropped entirely at the boundary (not used for analytics)
- Right-to-be-forgotten implemented as `scripts/gdpr_delete.py`:
  ```bash
  # Always preview first
  python scripts/gdpr_delete.py --store-id STR0042 --operator jane.doe --dry-run

  # Then execute with an audit reason
  python scripts/gdpr_delete.py --store-id STR0042 --operator jane.doe \
      --reason "Owner request #GDPR-2026-0147"
  ```
- Every deletion writes a row to `raw.gdpr_audit_log` with exact row counts

## Running Locally

**Prerequisites:** Docker Desktop, Python 3.11+, Git.

```bash
# 1. Start all services (Postgres warehouse + Airflow webserver/scheduler)
echo "AIRFLOW_UID=50000" > .env
docker compose up -d

# 2. Initialize the warehouse schema
docker compose exec -T postgres psql -U postgres -d retail \
    < sql/schema/00_extensions.sql
docker compose exec -T postgres psql -U postgres -d retail \
    < sql/schema/01_create_schemas.sql
docker compose exec -T postgres psql -U postgres -d retail \
    < sql/schema/02_create_warehouse.sql
docker compose exec -T postgres psql -U postgres -d retail \
    < sql/schema/03_create_audit_log.sql

# 3. Seed data (optional — the DAG will produce its own on first run)
pip install -r requirements.txt
python data_generator/generate_sales.py --days 90
docker compose exec -T postgres psql -U postgres -d retail \
    < sql/transformations/03_transform_raw_to_warehouse.sql

# 4. Open Airflow at http://localhost:8080 (admin / admin)
#    Enable the 'retail_sales_pipeline' DAG and trigger it manually.
```

**Verify it worked:**

```sql
-- Connect via DataGrip/DBeaver to localhost:5432, user=postgres, pass=postgres
SELECT ds.region, SUM(fs.total_amount) AS revenue
FROM warehouse.fact_sales fs
JOIN warehouse.dim_store ds ON ds.store_key = fs.store_key
GROUP BY ds.region
ORDER BY revenue DESC;
```

## Running the Tests

```bash
# Apply schema + seed data first (see "Running Locally" above), then:
pytest tests/ -v
```

Expected: 11 tests pass in under a second.

## Project Structure

```
retail-sales-pipeline/
├── .github/workflows/ci.yml       # GitHub Actions CI pipeline
├── dags/
│   └── retail_sales_pipeline.py   # The main Airflow DAG
├── data_generator/
│   └── generate_sales.py          # Faker-based source simulator
├── docs/                          # Screenshots for this README
├── scripts/
│   └── gdpr_delete.py             # Right-to-be-forgotten CLI
├── sql/
│   ├── schema/                    # CREATE TABLE statements (ordered 00–03)
│   └── transformations/
│       └── 03_transform_raw_to_warehouse.sql
├── tests/
│   └── test_data_quality.py       # 11 pytest assertions
├── docker-compose.yml             # Postgres + Airflow stack
├── GDPR.md                        # Data governance policy
└── README.md                      # This file
```

## Tech Choices & Why

| Tool | Why it was chosen |
|---|---|
| **PostgreSQL** | Free, universal, handles the warehouse workload at this scale. Also used by Snowflake-compatible dialects — easy migration path. |
| **Apache Airflow** | Industry standard for ETL orchestration. Visual DAGs + retry semantics + huge ecosystem. |
| **Docker Compose** | One-command reproducible environment. Anyone cloning the repo gets the same stack. |
| **GitHub Actions** | Free for public repos, tight GitHub integration, service-container support makes DB integration tests trivial. |
| **pytest** | Familiar to every Python developer, much cleaner than writing DQ checks as SQL scripts. |
| **SHA-256 for PII** | One-way hash, deterministic, supports use cases like "count distinct owners" without exposing identity. |

## What This Project Demonstrates

Mapped to common Data Engineer job requirements:

- **SQL & relational databases** — Star schema design, surrogate keys, referential integrity, composite indexes, check constraints, transactional DDL.
- **Python for data engineering** — Data generation with Faker, DAG authoring, DB connectivity with psycopg2, CLI tools with argparse.
- **ETL/ELT orchestration** — Apache Airflow DAG with task dependencies, retry policies, scheduling, and observability.
- **Data modeling & warehousing** — Kimball dimensional modeling, grain definition, medallion architecture (raw → warehouse), slowly-changing-dimension-ready schema.
- **Performance optimization** — Index design based on query pattern analysis; indexing tradeoffs documented in schema comments.
- **CI/CD** — GitHub Actions pipeline: lint → integration test with ephemeral Postgres → data quality suite → smoke tests.
- **Data governance & GDPR** — Layer-boundary pseudonymization, right-to-be-forgotten workflow, immutable audit log, data minimization principle applied.

## Limitations & Honest Scope

This is a portfolio project, not production. Things a real production
version would add:

- Source data from a real system (SAP, Fivetran, Kafka) instead of Faker
- dbt for the transformation layer (better lineage + docs than raw SQL)
- Remote Airflow (MWAA, Astronomer, or self-hosted on K8s) instead of Docker Compose
- Encryption at rest (TDE) and TLS on all DB connections
- A real BI tool (Power BI, Tableau, Looker) connected to the warehouse
- Alerting via Slack/PagerDuty on DAG failure
- Monitoring via Grafana + Prometheus

The patterns demonstrated here are the foundation those additions sit on.

## What I Learned

- **Idempotency is everything.** My first version wasn't idempotent and
  retries created duplicate data. Wrapping the transformation in a
  transaction + using `ON CONFLICT DO NOTHING` on the fact load fixed it.
- **Self-healing beats documentation.** My dimension load originally
  required manual seeding on first run. After one painful debug session
  I made it self-heal: if the target is empty, load regardless of append
  flag. One class of first-run failures eliminated.
- **Indexes are a tradeoff, not a free win.** I initially added an index
  per column. Switching to composite indexes based on actual query
  patterns (store × date, product × date) made analytics queries faster
  *and* inserts faster.
- **GDPR is more design than code.** The hardest part wasn't writing
  the deletion script — it was deciding where the pseudonymization boundary
  lives. Once "hash at the layer transition" clicked, the implementation
  was straightforward.

---

*Built by Giorgi Andriashvili. Questions or feedback — open an issue.*
