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
ENGINE = MergeTree() -- إضافة الأقواس هنا
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
-- Fact Table
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
ENGINE = MergeTree() -- إضافة الأقواس هنا
PARTITION BY report_year
ORDER BY (
    report_year,
    disease_key,
    location_key,
    outbreak_id,
    don_id
);