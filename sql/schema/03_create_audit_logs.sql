-- =============================================================================
-- 03_create_audit_log.sql
-- Audit log for GDPR deletion events. Stored in the `raw` schema because
-- it records operations on personal data in that layer.
--
-- Every right-to-be-forgotten execution must write one row here.
-- Auditors will check that for every deletion claim, this table has
-- a matching record — otherwise the claim is unprovable.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.gdpr_audit_log (
    audit_id              BIGSERIAL PRIMARY KEY,
    deleted_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    subject_store_id      TEXT NOT NULL,
    operator              TEXT NOT NULL,
    reason                TEXT,
    raw_stores_deleted    INTEGER NOT NULL,
    raw_sales_deleted     INTEGER NOT NULL,
    wh_stores_deleted     INTEGER NOT NULL,
    wh_sales_deleted      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gdpr_audit_subject
    ON raw.gdpr_audit_log(subject_store_id);

CREATE INDEX IF NOT EXISTS idx_gdpr_audit_deleted_at
    ON raw.gdpr_audit_log(deleted_at);