# Setup

Quickstart for anyone cloning this repo. Target: running pipeline in under
10 minutes on a clean machine.

## Requirements

- Docker Desktop (Windows: requires WSL2)
- Python 3.11+
- Git

## Step 1 — Clone & configure

```bash
git clone https://github.com/YOUR_USERNAME/retail-sales-pipeline.git
cd retail-sales-pipeline
echo "AIRFLOW_UID=50000" > .env
mkdir -p logs
```

## Step 2 — Start services

```bash
docker compose up -d
```

First run downloads ~1.5 GB of images (Postgres, Airflow). Expect 3–5 minutes.

Check everything's healthy:

```bash
docker compose ps
# Expect: retail_postgres, airflow_metadata, airflow_webserver,
#         airflow_scheduler all 'running' / 'healthy'
#         airflow_init 'exited (0)' — this is correct, it runs once
```

## Step 3 — Apply the schema

```bash
for f in 00_extensions 01_create_schemas 02_create_warehouse 03_create_audit_log; do
    docker compose exec -T postgres psql -U postgres -d retail \
        -v ON_ERROR_STOP=1 < sql/schema/${f}.sql
done
```

## Step 4 — Open Airflow

Browse to **http://localhost:8080** (user `admin`, password `admin`).

1. Click the toggle next to `retail_sales_pipeline` to unpause.
2. Click the ▶ button → "Trigger DAG" for a manual run.
3. Switch to the **Graph** tab to watch the four tasks run green.

## Step 5 — Query the warehouse

Connect any SQL client to `localhost:5432`, database `retail`,
user `postgres`, password `postgres`. Or from the terminal:

```bash
docker compose exec postgres psql -U postgres -d retail -c "
    SELECT ds.region, COUNT(*) AS txn_count,
           SUM(fs.total_amount) AS revenue
    FROM warehouse.fact_sales fs
    JOIN warehouse.dim_store ds ON ds.store_key = fs.store_key
    GROUP BY ds.region
    ORDER BY revenue DESC;
"
```

## Running the test suite

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Stopping

```bash
docker compose down          # stop services, keep data
docker compose down -v       # stop services, wipe data volumes
```

## Troubleshooting

**"permission denied" on `/opt/airflow/logs`** — the `.env` file is missing
or has the wrong UID. Run `echo "AIRFLOW_UID=50000" > .env` and
`docker compose down -v && docker compose up -d`.

**DAG appears with "import error"** — the compose file pip-installs
dependencies on first boot; takes up to 60 seconds. Refresh the UI.

**`generate_daily_sales` task fails with connection error** — Postgres
isn't ready. Check `docker compose ps` shows `retail_postgres` as
healthy; if not, `docker compose logs postgres`.

**Everything else** — `docker compose logs <service_name>` shows what
that container is complaining about.
