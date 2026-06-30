CREATE DATABASE IF NOT EXISTS data_warehouse_db;

USE data_warehouse_db;

-- ============================================
-- Dimension: Location
-- ============================================
CREATE TABLE IF NOT EXISTS dim_location
(
    location_key String,
    iso3 FixedString(3),
    country String,
    region_code Nullable(String),
    unsd_region Nullable(String),
    unsd_subregion Nullable(String)
)
ENGINE = MergeTree()
ORDER BY location_key;

-- ============================================
-- Dimension: Disease
-- ============================================
CREATE TABLE IF NOT EXISTS dim_disease
(
    disease_key Int32,
    disease_name String,
    definition Nullable(String),
    icd10_general Nullable(String),
    icd104_specific Nullable(String),
    start_date Date,
    end_date Nullable(Date),
    is_current UInt8
)
ENGINE = MergeTree()
ORDER BY disease_key;

-- ============================================
-- Fact Table (Historical Data)
-- ============================================
CREATE TABLE IF NOT EXISTS fact_outbreaks
(
    outbreak_id String,
    location_key String,
    disease_key Int32,
    report_year Int32,
    don_id String,
    outbreak_count Int32 DEFAULT 1,
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY report_year
ORDER BY (
    report_year,
    disease_key,
    location_key,
    outbreak_id,
    don_id
);

-- ============================================
-- Streaming Fact Table (Live Alerts)
-- ============================================
CREATE TABLE IF NOT EXISTS fact_outbreak_alerts
(
    alert_id UUID DEFAULT generateUUIDv4(),
    disease_name String,
    country String,
    published_at DateTime,
    ingestion_time DateTime DEFAULT now(),
    title String,
    source String,
    url String
)
ENGINE = MergeTree()
ORDER BY (country, disease_name, published_at);