# Retail Sales Intelligence Pipeline

An end-to-end data engineering project simulating a tobacco distributor's retail operations. Built to demonstrate modern data engineering practices: ETL orchestration, dimensional modeling, data quality, CI/CD, and GDPR-compliant PII handling.

## Business Problem

Tobacco distributors sell products through thousands of retail points (kiosks, supermarkets, gas stations). Tracking what's selling where, identifying low-stock situations, and measuring promotional effectiveness requires reliable, well-modeled data flowing from many sources into a centralized warehouse.

This project simulates that environment: daily sales transactions from ~50 fake stores are generated, ingested into a Postgres warehouse, transformed into a star schema, validated for quality, and made available for analytics — all orchestrated by Apache Airflow.

## Architecture

```
┌────────────────┐
│ Data Generator │  Python + Faker → realistic fake CSVs
│   (Python)     │
└───────┬────────┘
        │
        ▼
┌────────────────┐
│   Postgres     │  raw schema — landing zone
│   raw schema   │
└───────┬────────┘
        │
        ▼
┌────────────────┐
│    Airflow     │  Orchestrates the daily pipeline
│      DAGs      │  (extract → load → transform → test)
└───────┬────────┘
        │
        ▼
┌────────────────┐
│   Postgres     │  warehouse schema — star schema
│ warehouse      │  fact_sales + dim_store, dim_product, dim_date
│   schema       │
└────────────────┘

Wrapped around everything:
  • GitHub Actions CI/CD — tests run on every push
  • PII pseudonymization for GDPR compliance
  • Data quality checks (not_null, unique, referential integrity)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow |
| Warehouse | PostgreSQL |
| Language | Python 3.10+ |
| Containerization | Docker + Docker Compose |
| CI/CD | GitHub Actions |
| Version Control | Git + GitHub |

## Project Structure

```
retail-sales-pipeline/
├── README.md                   # You are here ;)
├── .gitignore
├── docker-compose.yml          # Spins up Postgres + Airflow (added in Step 2)
├── requirements.txt            # Python dependencies
├── dags/                       # Airflow DAG definitions
├── data_generator/             # Fake data generation scripts
├── sql/
│   ├── schema/                 # CREATE TABLE statements
│   └── transformations/        # raw → warehouse transformations
├── tests/                      # Python + SQL tests
└── .github/
    └── workflows/
        └── ci.yml              # GitHub Actions CI pipeline
```

## How to Run Locally

Prerequisites: Docker Desktop, Python 3.10+, Git

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/retail-sales-pipeline.git
cd retail-sales-pipeline

# 2. (Step 2) Spin up Postgres + Airflow
docker compose up -d

# 3. (Step 2) Initialize the schema
docker compose exec postgres psql -U pipeline -d retail -f /sql/schema/01_create_schemas.sql

# 4. (Step 3) Generate seed data
python data_generator/generate_sales.py
```

## Data Model (planned for Step 2)

The warehouse uses a classic star schema:

- **`fact_sales`** — one row per transaction (grain: store × product × timestamp)
- **`dim_store`** — store attributes (with pseudonymized owner contact)
- **`dim_product`** — SKU attributes (brand, category, list price)
- **`dim_date`** — calendar dimension (year, quarter, month, weekday)

Foreign keys enforce referential integrity. Indexes on join columns optimize query performance.

## Data Quality (planned)

Every pipeline run executes a suite of checks:
- Primary keys are unique and not null
- Foreign keys reference valid dimension records
- Numeric fields fall within expected ranges
- Daily row counts are within historical bounds (anomaly detection)

## GDPR & Data Governance (planned)

Personal data (store owner names, emails, phone numbers) is **pseudonymized** during the load step using SHA-256 hashing with a salt. The original values never enter the warehouse.

A `gdpr_delete.py` utility script will support the "right to be forgotten".

## CI/CD

GitHub Actions runs on every push and pull request:
1. **Lint** — `flake8` checks Python code style
2. **Unit tests** — `pytest` runs Python unit tests
3. **SQL validation** — checks that all SQL files parse correctly (later step)

## What I Learned

*(To be filled in as the project progresses.)*

## Author

Built as a portfolio project to demonstrate data engineering fundamentals.
