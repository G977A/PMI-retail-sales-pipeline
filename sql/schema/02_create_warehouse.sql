-- =============================================================================
-- 02_create_warehouse.sql
-- Star schema for analytics. This is the layer BI tools and analysts query.
--
-- Design principles:
--   1. Surrogate keys (BIGSERIAL) on dimensions, not natural keys.
--      Reason: source systems can change their IDs; surrogates insulate us.
--   2. Foreign keys enforced — analysts trust referential integrity.
--   3. NOT NULL on every column that should never be missing.
--   4. Indexes on every FK + on common filter columns (date, region).
--   5. PII (owner_*) replaced with hashed versions in this layer.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIM_DATE — calendar dimension
-- Pre-built so queries can JOIN on date_key instead of doing EXTRACT() everywhere.
-- This is faster AND lets analysts filter by quarter/weekday without writing SQL.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS warehouse.dim_date CASCADE;
CREATE TABLE warehouse.dim_date (
    date_key        DATE PRIMARY KEY,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      TEXT NOT NULL,
    day             SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,    -- 1 = Monday
    day_name        TEXT NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

-- -----------------------------------------------------------------------------
-- DIM_STORE — store master
-- Note: owner_* columns are SHA-256 hashes of the source PII (GDPR compliance).
-- The raw layer keeps the original PII for legitimate operational use; the
-- warehouse layer (which feeds analytics & BI) only sees pseudonymized values.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS warehouse.dim_store CASCADE;
CREATE TABLE warehouse.dim_store (
    store_key       BIGSERIAL PRIMARY KEY,
    store_id        TEXT NOT NULL UNIQUE,    -- natural key from source
    store_name      TEXT NOT NULL,
    store_type      TEXT NOT NULL,
    city            TEXT NOT NULL,
    region          TEXT NOT NULL,
    owner_name_hash TEXT,                    -- SHA-256, GDPR pseudonymized
    owner_email_hash TEXT,                   -- SHA-256, GDPR pseudonymized
    opened_date     DATE,
    loaded_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dim_store_region ON warehouse.dim_store(region);
CREATE INDEX idx_dim_store_type   ON warehouse.dim_store(store_type);

-- -----------------------------------------------------------------------------
-- DIM_PRODUCT — product catalog
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS warehouse.dim_product CASCADE;
CREATE TABLE warehouse.dim_product (
    product_key     BIGSERIAL PRIMARY KEY,
    product_id      TEXT NOT NULL UNIQUE,
    sku             TEXT NOT NULL,
    brand           TEXT NOT NULL,
    product_name    TEXT NOT NULL,
    category        TEXT NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    loaded_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dim_product_brand    ON warehouse.dim_product(brand);
CREATE INDEX idx_dim_product_category ON warehouse.dim_product(category);

-- -----------------------------------------------------------------------------
-- FACT_SALES — the central fact table
-- Grain: one row per individual transaction.
-- All FKs are enforced. Measures are NUMERIC for accuracy.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS warehouse.fact_sales CASCADE;
CREATE TABLE warehouse.fact_sales (
    sale_key        BIGSERIAL PRIMARY KEY,
    transaction_id  TEXT NOT NULL UNIQUE,    -- natural key, used for idempotent loads
    store_key       BIGINT NOT NULL REFERENCES warehouse.dim_store(store_key),
    product_key     BIGINT NOT NULL REFERENCES warehouse.dim_product(product_key),
    date_key        DATE NOT NULL REFERENCES warehouse.dim_date(date_key),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    total_amount    NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    loaded_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Performance indexes on the fact table.
-- These cover the three most common query patterns in retail analytics:
--   1. "Sales for store X over time"        -> (store_key, date_key)
--   2. "Sales for product Y over time"      -> (product_key, date_key)
--   3. "Sales for date range across all"    -> (date_key)
CREATE INDEX idx_fact_sales_store_date    ON warehouse.fact_sales(store_key, date_key);
CREATE INDEX idx_fact_sales_product_date  ON warehouse.fact_sales(product_key, date_key);
CREATE INDEX idx_fact_sales_date          ON warehouse.fact_sales(date_key);
