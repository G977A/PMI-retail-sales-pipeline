-- =============================================================================
-- 01_create_schemas.sql
-- Creates the two-layer architecture:
--   raw       — landing zone for source data, minimal constraints
--   warehouse — clean, modeled, indexed star schema for analytics
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- =============================================================================
-- RAW LAYER
-- Mirrors the shape of source systems. We deliberately keep this loose:
-- no foreign keys, nullable columns, original data types. The job of the
-- raw layer is to faithfully capture what arrived, even if it's messy.
-- =============================================================================

DROP TABLE IF EXISTS raw.stores CASCADE;
CREATE TABLE raw.stores (
    store_id        TEXT,
    store_name      TEXT,
    store_type      TEXT,        -- kiosk, supermarket, gas_station
    city            TEXT,
    region          TEXT,
    owner_name      TEXT,        -- PII
    owner_email     TEXT,        -- PII
    owner_phone     TEXT,        -- PII
    opened_date     TEXT,        -- stored as text on purpose: source files often have inconsistent formats
    ingested_at     TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS raw.products CASCADE;
CREATE TABLE raw.products (
    product_id      TEXT,
    sku             TEXT,
    brand           TEXT,
    product_name    TEXT,
    category        TEXT,
    unit_price      TEXT,        -- stored as text to preserve source fidelity
    ingested_at     TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS raw.sales CASCADE;
CREATE TABLE raw.sales (
    transaction_id  TEXT,
    store_id        TEXT,
    product_id      TEXT,
    sale_date       TEXT,
    quantity        TEXT,
    unit_price      TEXT,
    total_amount    TEXT,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- Indexes on raw layer: only what we need for the load step.
-- We keep raw lean — heavy indexing belongs in the warehouse layer.
CREATE INDEX IF NOT EXISTS idx_raw_sales_ingested ON raw.sales(ingested_at);
