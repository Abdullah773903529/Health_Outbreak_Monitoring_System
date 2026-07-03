-- 1. جدول الأبعاد (معدل: يدعم ReplacingMergeTree لمنع التكرار التلقائي)
CREATE TABLE IF NOT EXISTS data_warehouse_db.dim_location
(
    location_key String,
    iso3 FixedString(3),
    country String,
    region_code Nullable(String),
    unsd_region Nullable(String),
    unsd_subregion Nullable(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY location_key;

-- 2. جدول الأمراض (معدل: يدعم String للـ Key ليتوافق مع MD5، ويدعم SCD2)
CREATE TABLE IF NOT EXISTS data_warehouse_db.dim_disease
(
    disease_key String,
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

-- 3. جدول الحقائق التاريخية (معدل: disease_key أصبح String)
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

-- 4. جدول الحقائق اللحظية (معدل: disease_key أصبح String)
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
ENGINE = MergeTree()
ORDER BY (published_at, disease_key, location_key);