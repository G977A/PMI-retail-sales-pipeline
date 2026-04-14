# GDPR & Data Governance

This document describes how the Retail Sales Pipeline handles personal data
to comply with the EU General Data Protection Regulation (GDPR) and analogous
regulations (Georgia's Law on Personal Data Protection, UK DPA, etc.).

## Scope of Personal Data

The pipeline ingests and processes one category of personal data:

| Field | Source | Classification | Where it lives |
|---|---|---|---|
| `owner_name` | `raw.stores` | PII — direct identifier | Raw layer only |
| `owner_email` | `raw.stores` | PII — direct identifier | Raw layer only |
| `owner_phone` | `raw.stores` | PII — direct identifier | Raw layer only |

No consumer-level data is collected. Sales transactions are anonymous —
they contain no personal identifiers, only `store_id` and `product_id`.

## Design Principles

### 1. Data Minimization
Personal data never crosses the boundary between the raw layer and the
warehouse layer in plaintext. The transformation step
(`sql/transformations/03_transform_raw_to_warehouse.sql`) applies SHA-256
hashing to `owner_name` and `owner_email` before loading them into
`warehouse.dim_store`. The `owner_phone` field is dropped entirely during
the transformation because it is never used for analytics.

### 2. Purpose Limitation
- **Raw layer** holds plaintext PII so operational teams can contact store
  owners (e.g., for deliveries, disputes, onboarding).
- **Warehouse layer** holds only pseudonymized hashes, used exclusively for
  analytics (e.g., "how many distinct owners operate in Tbilisi?").

Analysts querying the warehouse cannot reverse-engineer the original PII.

### 3. Storage Limitation
Raw data is retained for 180 days, after which store records without
recent activity are archived. Warehouse hashes are retained indefinitely
because they contain no recoverable PII.

(Note: in this portfolio project the retention policy is documented but
not automated. In production it would be enforced by a scheduled cleanup
DAG.)

### 4. Accountability
Every deletion request is logged in `raw.gdpr_audit_log` with:
- Timestamp of the deletion
- Identifier of the subject whose data was removed
- Row counts affected in each table
- The operator who initiated the deletion

## Right to Be Forgotten (Article 17)

Store owners can request deletion of their data. The workflow:

1. Operator confirms the request is legitimate (identity verification happens
   outside this system).
2. Operator runs the deletion script:
   ```bash
   python scripts/gdpr_delete.py --store-id STR0042 --operator jane.doe
   ```
3. The script:
   - Deletes all matching rows from `raw.stores`
   - Deletes all matching rows from `warehouse.dim_store`
   - Deletes all related sales from `raw.sales` and `warehouse.fact_sales`
     (so historical transactions tied to the deleted owner are removed too)
   - Records the action in `raw.gdpr_audit_log`
4. The operator receives a summary confirming row counts deleted.

The deletion is irreversible by design. There is no soft-delete flag; data
is hard-removed.

## Right to Access (Article 15)

Operators can retrieve all stored data for a given owner using the
`warehouse.vw_owner_data_export` view (not yet implemented — planned).
Because warehouse data is hashed, this query is run against the raw layer.

## Subprocessor & Cross-Border Transfer

This project is self-hosted. No data leaves the pipeline's boundaries.
In a production deployment, any cloud provider, BI tool, or analytics
consumer that touches personal data would be documented here as a
subprocessor, and Standard Contractual Clauses would govern transfers
outside the EU/EEA.

## Limitations & Honest Scope

This is a portfolio project. A production-grade GDPR implementation
would additionally include:

- Encryption at rest (Postgres TDE or disk-level encryption)
- Encryption in transit enforced via `sslmode=require` on all connections
- Automated retention enforcement via a scheduled cleanup DAG
- Row-level access controls for operators vs. analysts
- A formal Data Protection Impact Assessment (DPIA)
- Integration with a data catalog (e.g., Collibra) for lineage tracking
- Regular penetration testing and access reviews

The patterns demonstrated here — layer-boundary pseudonymization, audit
logging, scripted deletion — are the building blocks of that larger
system.