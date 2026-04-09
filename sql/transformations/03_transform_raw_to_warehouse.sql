-- =============================================================================
-- 03_transform_raw_to_warehouse.sql
-- Loads cleaned, modeled data from the raw layer into the warehouse star schema.
--
-- Design principles:
--   1. Idempotent — running this script twice produces the same result.
--      We use TRUNCATE + INSERT for dimensions (small) and an upsert pattern
--      for the fact table (larger, using ON CONFLICT for transaction_id).
--   2. PII pseudonymized — owner_name and owner_email are SHA-256 hashed
--      before they ever land in the warehouse layer. The original PII never
--      leaves the raw schema.
--   3. Type-safe — every TEXT column from raw is explicitly cast to its
--      proper warehouse type, with bad rows rejected (not silently coerced).
--   4. Surrogate keys resolved at load time — fact_sales joins back to
--      dim_store / dim_product / dim_date to look up the integer keys.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- STEP 1: Populate dim_date
-- We generate a calendar from the earliest sale to today + 30 days of buffer.
-- generate_series is a Postgres builtin that creates a sequence of values.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE warehouse.dim_date CASCADE;

INSERT INTO warehouse.dim_date (
    date_key, year, quarter, month, month_name,
    day, day_of_week, day_name, is_weekend
)
SELECT
    d::date                                    AS date_key,
    EXTRACT(year    FROM d)::smallint          AS year,
    EXTRACT(quarter FROM d)::smallint          AS quarter,
    EXTRACT(month   FROM d)::smallint          AS month,
    TO_CHAR(d, 'Month')                        AS month_name,
    EXTRACT(day     FROM d)::smallint          AS day,
    EXTRACT(isodow  FROM d)::smallint          AS day_of_week,
    TO_CHAR(d, 'Day')                          AS day_name,
    EXTRACT(isodow  FROM d) IN (6, 7)          AS is_weekend
FROM generate_series(
    (SELECT MIN(sale_date::date) FROM raw.sales),
    (SELECT MAX(sale_date::date) FROM raw.sales) + INTERVAL '30 days',
    INTERVAL '1 day'
) AS d;


-- -----------------------------------------------------------------------------
-- STEP 2: Load dim_store with PII pseudonymization
-- This is the GDPR-compliant load. We hash owner_name and owner_email with
-- SHA-256 so the warehouse layer cannot reverse-engineer the original PII.
--
-- The raw layer keeps the original PII for legitimate operational use
-- (e.g., contacting a store owner about a delivery). The warehouse layer,
-- which feeds analytics, only sees opaque hashes.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE warehouse.dim_store RESTART IDENTITY CASCADE;

INSERT INTO warehouse.dim_store (
    store_id, store_name, store_type, city, region,
    owner_name_hash, owner_email_hash, opened_date
)
SELECT
    store_id,
    store_name,
    store_type,
    city,
    region,
    encode(digest(owner_name,  'sha256'), 'hex')  AS owner_name_hash,
    encode(digest(owner_email, 'sha256'), 'hex')  AS owner_email_hash,
    opened_date::date
FROM raw.stores
WHERE store_id IS NOT NULL;


-- -----------------------------------------------------------------------------
-- STEP 3: Load dim_product
-- Straightforward type casting with validation. The CHECK constraint on
-- unit_price (in the schema) will reject any negative prices.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE warehouse.dim_product RESTART IDENTITY CASCADE;

INSERT INTO warehouse.dim_product (
    product_id, sku, brand, product_name, category, unit_price
)
SELECT
    product_id,
    sku,
    brand,
    product_name,
    category,
    unit_price::numeric(10, 2)
FROM raw.products
WHERE product_id IS NOT NULL;


-- -----------------------------------------------------------------------------
-- STEP 4: Load fact_sales
-- This is the most interesting transformation. For each raw transaction, we:
--   - Cast all text columns to their proper types
--   - Look up the surrogate keys from each dimension via JOINs
--   - Use ON CONFLICT (transaction_id) DO NOTHING for idempotent loads
--     (running this twice won't create duplicate fact rows)
--
-- The INNER JOINs to dimensions also act as a data quality filter:
-- if a sale references a store_id or product_id that doesn't exist
-- in the dimensions, the sale is silently dropped. In production we'd
-- log these to a quarantine table, but for this project we keep it simple.
-- -----------------------------------------------------------------------------
INSERT INTO warehouse.fact_sales (
    transaction_id, store_key, product_key, date_key,
    quantity, unit_price, total_amount
)
SELECT
    s.transaction_id,
    ds.store_key,
    dp.product_key,
    s.sale_date::date         AS date_key,
    s.quantity::integer       AS quantity,
    s.unit_price::numeric(10, 2),
    s.total_amount::numeric(12, 2)
FROM raw.sales s
INNER JOIN warehouse.dim_store   ds ON ds.store_id   = s.store_id
INNER JOIN warehouse.dim_product dp ON dp.product_id = s.product_id
INNER JOIN warehouse.dim_date    dd ON dd.date_key   = s.sale_date::date
WHERE s.transaction_id IS NOT NULL
  AND s.quantity::integer > 0
ON CONFLICT (transaction_id) DO NOTHING;


-- -----------------------------------------------------------------------------
-- STEP 5: Sanity check — print row counts so we can verify the load worked.
-- These show up in the Airflow logs when the DAG runs this script.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    raw_sales_count       BIGINT;
    fact_sales_count      BIGINT;
    dim_store_count       BIGINT;
    dim_product_count     BIGINT;
    dim_date_count        BIGINT;
BEGIN
    SELECT COUNT(*) INTO raw_sales_count    FROM raw.sales;
    SELECT COUNT(*) INTO fact_sales_count   FROM warehouse.fact_sales;
    SELECT COUNT(*) INTO dim_store_count    FROM warehouse.dim_store;
    SELECT COUNT(*) INTO dim_product_count  FROM warehouse.dim_product;
    SELECT COUNT(*) INTO dim_date_count     FROM warehouse.dim_date;

    RAISE NOTICE 'Load summary:';
    RAISE NOTICE '  raw.sales:           % rows', raw_sales_count;
    RAISE NOTICE '  warehouse.fact_sales: % rows (% rejected)',
        fact_sales_count, (raw_sales_count - fact_sales_count);
    RAISE NOTICE '  warehouse.dim_store:  % rows', dim_store_count;
    RAISE NOTICE '  warehouse.dim_product: % rows', dim_product_count;
    RAISE NOTICE '  warehouse.dim_date:    % rows', dim_date_count;
END $$;

COMMIT;
