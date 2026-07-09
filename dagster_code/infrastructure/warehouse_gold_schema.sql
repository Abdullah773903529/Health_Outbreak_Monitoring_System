-- ============================================================
-- قاعدة بيانات: data_warehouse_db
-- الغرض: تخزين وتحليل بيانات تفشي الأمراض
-- التاريخ: 2026-07-09
-- ============================================================

-- استخدام قاعدة البيانات
USE data_warehouse_db;

-- ============================================================
-- 1. جدول الأبعاد: المواقع (dim_location)
-- ============================================================
CREATE TABLE IF NOT EXISTS data_warehouse_db.dim_location
(
    location_key String,           -- المفتاح الأساسي للموقع
    iso3 FixedString(3),           -- رمز الدولة ISO3 (مثل USA, GBR)
    country String,                -- اسم الدولة
    region_code String,            -- رمز المنطقة (NA, EU, SA, AF)
    unsd_region String,            -- منطقة UNSD
    unsd_subregion String          -- المنطقة الفرعية UNSD
)
ENGINE = ReplacingMergeTree()      -- تحديث السجلات المكررة
PARTITION BY region_code           -- تقسيم حسب المنطقة
ORDER BY (region_code, country, location_key)  -- ترتيب للبحث السريع
SETTINGS
    index_granularity = 8194,
    min_rows_for_wide_part = 100000;

-- ============================================================
-- 2. جدول الأبعاد: الأمراض (dim_disease)
-- ============================================================
CREATE TABLE IF NOT EXISTS data_warehouse_db.dim_disease
(
    disease_key String,            -- المفتاح الأساسي للمرض
    disease_name String,           -- اسم المرض
    definition String,             -- تعريف المرض
    icd10_general String,          -- كود ICD-10 العام
    icd104_specific String,        -- كود ICD-10 المحدد
    start_date Date,               -- تاريخ بدء المرض
    end_date Nullable(Date),       -- تاريخ انتهاء المرض (يمكن أن يكون فارغ)
    is_current UInt8               -- هل المرض حالياً (1 = نعم، 0 = لا)
)
ENGINE = MergeTree()               -- محرك MergeTree الأساسي
PARTITION BY toYear(start_date)    -- تقسيم حسب سنة البداية
ORDER BY (disease_key, disease_name)  -- ترتيب للبحث السريع
SETTINGS
    index_granularity = 8194,
    min_rows_for_wide_part = 100000;

-- ============================================================
-- 3. جدول الحقائق: تفشي الأمراض (fact_outbreaks)
-- ============================================================
CREATE TABLE IF NOT EXISTS data_warehouse_db.fact_outbreaks
(
    outbreak_id String,            -- معرف التفشي
    location_key String,           -- مفتاح الموقع (يرتبط بـ dim_location)
    disease_key String,            -- مفتاح المرض (يرتبط بـ dim_disease)
    report_year Int32,             -- سنة التقرير (للتقسيم)
    don_id String,                 -- معرف منظمة الصحة (DON)
    outbreak_count Int32 DEFAULT 1, -- عدد الحالات (افتراضي 1)
    ingestion_timestamp DateTime DEFAULT now()  -- وقت الإدخال
)
ENGINE = MergeTree()               -- محرك MergeTree الأساسي
PARTITION BY report_year           -- ⭐ تقسيم حسب سنة التقرير
ORDER BY (report_year, disease_key, location_key, outbreak_id)  -- ترتيب متعدد الأبعاد
SETTINGS
    index_granularity = 8194,
    min_rows_for_wide_part = 100000,
    max_parts_in_total = 100000,
    max_bytes_to_merge_at_max_space_in_pool = 10737418240;



    
CREATE TABLE data_warehouse_db.fact_outbreak_alerts
(
    alert_id String,
    disease_key String,            
    location_key String,           
    published_at DateTime,         
    ingestion_time DateTime,  
    title String,                  
    source String,                 
    url String                     
)
ENGINE = ReplacingMergeTree(ingestion_time)
PARTITION BY toYYYYMM(published_at)
ORDER BY (source, location_key, published_at, disease_key);

-- 4.2 إضافة الـ Projection
ALTER TABLE data_warehouse_db.fact_outbreak_alerts
ADD PROJECTION IF NOT EXISTS alerts_by_location
(
    SELECT location_key, disease_key, count() as alert_count
    GROUP BY location_key, disease_key
);

-- 4.3 إضافة الـ Skip Index
ALTER TABLE data_warehouse_db.fact_outbreak_alerts
ADD INDEX IF NOT EXISTS disease_location_idx (disease_key, location_key) TYPE bloom_filter GRANULARITY 4;

-- 4.4 تفعيل الـ Projection (مادياً)
ALTER TABLE data_warehouse_db.fact_outbreak_alerts
MATERIALIZE PROJECTION alerts_by_location;