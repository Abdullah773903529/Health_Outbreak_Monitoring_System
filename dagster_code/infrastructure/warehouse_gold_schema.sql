-- تنظيف الجداول القديمة لضمان بناء هيكل نظيف مطابق للطبقة الفضية
DROP TABLE IF EXISTS fact_outbreaks CASCADE;
DROP TABLE IF EXISTS dim_disease CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;

-- 1. جدول الأبعاد المكانية (Dimension: Location)
-- متطابق مع الأعمدة الجغرافية في الطبقة الفضية
CREATE TABLE dim_location (
    location_key VARCHAR(255) PRIMARY KEY, -- يتم توليده عبر التشفير (Hash) في Spark
    iso3 VARCHAR(3) NOT NULL,
    country VARCHAR(255) NOT NULL,
    region_code VARCHAR(255),              -- تم التعديل ليطابق withColumnRenamed
    unsd_region VARCHAR(255),
    unsd_subregion VARCHAR(255)
);

-- 2. جدول الأبعاد الطبية (Dimension: Disease)
-- متطابق مع البيانات الطبية مع دعم التغيرات التاريخية (SCD Type 2)
CREATE TABLE dim_disease (
    disease_key SERIAL PRIMARY KEY,
    disease_name VARCHAR(255) NOT NULL,
    definition TEXT,
    icd10_general VARCHAR(50),
    icd104_specific VARCHAR(50),           -- تمت إضافته بناءً على كود Silver
    start_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);

-- 3. جدول الحقائق المركزي (Fact: Outbreaks)
-- متطابق مع المقاييس والأرقام المتوفرة
CREATE TABLE fact_outbreaks (
    outbreak_id VARCHAR(255) PRIMARY KEY,
    location_key VARCHAR(255) REFERENCES dim_location(location_key),
    disease_key INTEGER REFERENCES dim_disease(disease_key),
    report_year INTEGER NOT NULL,          -- يطابق عمود year في الـ Silver
    don_id VARCHAR(100),
    outbreak_count INTEGER DEFAULT 1,      -- يطابق العمود المضاف في الـ Silver
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- إضافة الفهارس (Indexes) لتحسين سرعة الاستعلامات في لوحة التحكم
CREATE INDEX idx_fact_year ON fact_outbreaks(report_year);
CREATE INDEX idx_fact_location ON fact_outbreaks(location_key);
CREATE INDEX idx_fact_disease ON fact_outbreaks(disease_key);