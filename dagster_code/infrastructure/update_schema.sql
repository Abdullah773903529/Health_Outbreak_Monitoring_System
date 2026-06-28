-- احذف جميع الجداول بالترتيب العكسي (لأن الحقائق تعتمد على الأبعاد)
DROP TABLE IF EXISTS fact_outbreaks CASCADE;
DROP TABLE IF EXISTS dim_disease CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;

-- الآن أعد بناء الجداول
CREATE TABLE dim_location (
    location_key VARCHAR(255) PRIMARY KEY,
    iso3 VARCHAR(3) NOT NULL,
    country VARCHAR(255) NOT NULL,
    region_code VARCHAR(255),
    unsd_region VARCHAR(255),
    unsd_subregion VARCHAR(255)
);

CREATE TABLE dim_disease (
    disease_key INTEGER PRIMARY KEY, -- تم تغييرها إلى Integer ليتناسب مع الكود
    disease_name VARCHAR(255) NOT NULL,
    definition TEXT,
    icd10_general VARCHAR(50),
    icd104_specific VARCHAR(50),
    start_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE fact_outbreaks (
    outbreak_id VARCHAR(255),
    location_key VARCHAR(255) REFERENCES dim_location(location_key),
    disease_key INTEGER REFERENCES dim_disease(disease_key),
    report_year INTEGER NOT NULL,
    don_id TEXT NOT NULL,
    outbreak_count INTEGER DEFAULT 1,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (outbreak_id, don_id)
);

-- الفهارس
CREATE INDEX idx_fact_year ON fact_outbreaks(report_year);
CREATE INDEX idx_fact_location ON fact_outbreaks(location_key);
CREATE INDEX idx_fact_disease ON fact_outbreaks(disease_key);