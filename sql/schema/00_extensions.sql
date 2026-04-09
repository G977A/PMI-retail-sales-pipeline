-- Enable pgcrypto for SHA-256 hashing in the GDPR pseudonymization step.
-- Only needs to run once per database.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
