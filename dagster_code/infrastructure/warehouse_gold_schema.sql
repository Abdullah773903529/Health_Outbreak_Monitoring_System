CREATE TABLE IF NOT EXISTS data_warehouse_db.dim_location
(
    location_key String,
    iso3 FixedString(3),
    country String,
    region_code(String),
    unsd_region (String),
    unsd_subregion (String)
)
ENGINE = ReplacingMergeTree()
ORDER BY location_key;

CREATE TABLE IF NOT EXISTS data_warehouse_db.dim_disease
(
    disease_key String,
    disease_name String,
    definition (String),
    icd10_general (String),
    icd104_specific (String),
    start_date Date,
    end_date Nullable(Date),
    is_current UInt8
)
ENGINE = MergeTree()
ORDER BY disease_key;

CREATE TABLE IF NOT EXISTS data_warehouse_db.fact_outbreaks
(
    outbreak_id String,
    location_key String,
    disease_key String,
    report_year Int32,
    don_id String,
    outbreak_count Int32 DEFAULT 1,
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY report_year
ORDER BY (report_year, disease_key, location_key, outbreak_id, don_id);

CREATE TABLE IF NOT EXISTS data_warehouse_db.fact_outbreak_alerts
(
    alert_id UUID,
    disease_key String,
    location_key String,
    published_at DateTime,
    ingestion_time DateTime DEFAULT now(),
    title String,
    source String,
    url String
)

ENGINE = ReplacingMergeTree(ingestion_time)
ORDER BY (disease_key, location_key, published_at, url);