CREATE TABLE IF NOT EXISTS organizations (
    id          SERIAL PRIMARY KEY,
    fhir_id     TEXT NOT NULL,
    raw_json    JSONB NOT NULL,
    source_dataset TEXT NOT NULL,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (fhir_id, source_dataset)
);

CREATE TABLE IF NOT EXISTS organization_affiliations (
    id          SERIAL PRIMARY KEY,
    fhir_id     TEXT NOT NULL,
    raw_json    JSONB NOT NULL,
    source_dataset TEXT NOT NULL,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (fhir_id, source_dataset)
);

CREATE TABLE IF NOT EXISTS practitioners (
    id          SERIAL PRIMARY KEY,
    fhir_id     TEXT NOT NULL,
    raw_json    JSONB NOT NULL,
    source_dataset TEXT NOT NULL,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (fhir_id, source_dataset)
);

CREATE TABLE IF NOT EXISTS practitioner_roles (
    id          SERIAL PRIMARY KEY,
    fhir_id     TEXT NOT NULL,
    raw_json    JSONB NOT NULL,
    source_dataset TEXT NOT NULL,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (fhir_id, source_dataset)
);

CREATE TABLE IF NOT EXISTS locations (
    id          SERIAL PRIMARY KEY,
    fhir_id     TEXT NOT NULL,
    raw_json    JSONB NOT NULL,
    source_dataset TEXT NOT NULL,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (fhir_id, source_dataset)
);

CREATE TABLE IF NOT EXISTS endpoints (
    id          SERIAL PRIMARY KEY,
    fhir_id     TEXT NOT NULL,
    raw_json    JSONB NOT NULL,
    source_dataset TEXT NOT NULL,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (fhir_id, source_dataset)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id                    SERIAL PRIMARY KEY,
    run_at                TIMESTAMPTZ DEFAULT now(),
    source_file           TEXT NOT NULL,
    source_dataset        TEXT NOT NULL,
    resource_type         TEXT NOT NULL,
    compression           TEXT NOT NULL,
    compressed_size_bytes BIGINT,
    download_ms           BIGINT,
    process_ms            BIGINT NOT NULL,
    total_ms              BIGINT NOT NULL,
    record_count          INT NOT NULL,
    error_count           INT NOT NULL,
    records_per_sec       FLOAT
);
