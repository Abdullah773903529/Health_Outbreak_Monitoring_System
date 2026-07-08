from dagster import asset, Output, MetadataValue, get_dagster_logger
from pyspark.sql import functions as F
from .utils import get_spark_session

@asset(
    deps=["bronze_raw_outbreak_data"],
    compute_kind="spark",
    group_name="batch_layer"
)
def silver_cleaned_outbreaks():
    """
    Asset: Silver Layer - Cleaned Outbreak Data
    This asset reads the bronze layer outbreak data, performs extensive cleaning and transformation,
    and writes the cleaned data to the silver layer in Parquet format. The cleaning steps include:
    """
    spark = get_spark_session("SilverCleaningJob")
    logger = get_dagster_logger()
    
    INPUT_PATH = 's3a://outbreak-data/bronze/outbreaks_parquet'
    OUTPUT_PATH = 's3a://outbreak-data/silver/outbreaks_cleaned_parquet'
    
    # ==========================================
    # 1. قراءة البيانات
    # ==========================================
    df = spark.read.parquet(INPUT_PATH)
    initial_count = df.count()
    logger.info(f" Bronze loaded: {initial_count} rows")

    # ==========================================
    # 2.  حذف أي صف به قيمة فارغة في أي عمود
    # ==========================================
    
    all_columns = df.columns
    logger.info(f" Columns: {all_columns}")
    
    df_cleaned = df.na.drop() # inplace drop of any row with NULL in any column
    
    after_null_drop = df_cleaned.count()
    null_dropped = initial_count - after_null_drop
    logger.info(f" Dropped {null_dropped} rows with NULL values")
    
    # ==========================================
    #  3.  حذف أي صف به قيمة فارغة (empty string) في أي عمود
    # ==========================================
    for col_name in all_columns:
        if col_name in df_cleaned.columns:
            df_cleaned = df_cleaned.filter(
                F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != "")
            )
    
    after_empty_drop = df_cleaned.count()
    empty_dropped = after_null_drop - after_empty_drop
    logger.info(f" Dropped {empty_dropped} rows with empty strings")

    # 4.  إزالة التكرارات
    # ==========================================
    if 'outbreak_id' in df_cleaned.columns:
        df_cleaned = df_cleaned.dropDuplicates(['outbreak_id'])
    
    after_dup_drop = df_cleaned.count()
    dup_dropped = after_empty_drop - after_dup_drop
    logger.info(f" Dropped {dup_dropped} duplicate rows")

    # 5.  فلترة السنة (> 1996 بدون حد أعلى)
    # ==========================================
    if 'year' in df_cleaned.columns:
        df_cleaned = df_cleaned.filter(F.col('year') >= 1996)
    
    after_year_drop = df_cleaned.count()
    year_dropped = after_dup_drop - after_year_drop
    logger.info(f" Dropped {year_dropped} rows with year < 1996")

    # 6.  توحيد النصوص (بدون تعبئة قيم افتراضية)
    # ==========================================
    
    #  أعمدة تحول لأحرف كبيرة مع إزالة الفراغات
    upper_trim_cols = [
        "disease_name", "country", "iso3", "icd10_general",
        "who_region", "unsd_region", "unsd_subregion"
    ]
    for col_name in upper_trim_cols:
        if col_name in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(col_name, F.trim(F.upper(F.col(col_name))))
    
    #  أعمدة إزالة الفراغات فقط
    trim_only_cols = ["definition", "don_id", "icd104_specific", "outbreak_id"]
    for col_name in trim_only_cols:
        if col_name in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(col_name, F.trim(F.col(col_name)))

    # 7.  تصحيح أخطاء الترميز في أسماء الدول
    # ==========================================
    if 'country' in df_cleaned.columns:
        df_cleaned = df_cleaned.withColumn(
            "country",
            F.when(F.col("country").like("C%TE D'IVOIRE"), "COTE D'IVOIRE")
             .when(F.col("country").like("R%UNION"), "REUNION")
             .when(F.col("country").like("CURA%AO"), "CURACAO")
             .when(F.col("country").like("SAINT BARTH%LEMY"), "SAINT BARTHELEMY")
             .otherwise(F.col("country"))
        )

    
    # 8. تعبئة الأقاليم الجغرافية المفقودة (لأنها أعمدة مهمة للـ Gold)
    # ==========================================
    if 'country' in df_cleaned.columns and 'who_region' in df_cleaned.columns:
        americas_countries = [
            'SAINT BARTHELEMY', 'ANGUILLA', 'CURACAO', 'BERMUDA',
            'SAINT PIERRE AND MIQUELON', 'BONAIRE SINT EUSTATIUS AND SABA',
            'MARTINIQUE', 'SAINT MARTIN (FRENCH PART)', 'MONTSERRAT',
            'SINT MAARTEN (DUTCH PART)', 'VIRGIN ISLANDS (BRITISH)',
            'FALKLAND ISLANDS (MALVINAS)', 'PUERTO RICO', 'FRENCH GUIANA',
            'VIRGIN ISLANDS (U.S.)', 'TURKS AND CAICOS ISLANDS',
            'ARUBA', 'GUADELOUPE', 'CAYMAN ISLANDS'
        ]
        europe_countries = [
            'KOSOVO', 'GREENLAND', 'HOLY SEE', 'JERSEY', 'GIBRALTAR',
            'FAROE ISLANDS', 'GUERNSEY', 'ISLE OF MAN', 'LIECHTENSTEIN'
        ]
        asia_countries = ['TAIWAN PROVINCE OF CHINA', 'MACAO', 'PALESTINE STATE OF', 'HONG KONG']
        africa_countries = ['SAINT HELENA ASCENSION AND TRISTAN DA CUNHA', 'REUNION', 'MAYOTTE']
        oceania_countries = [
            'TOKELAU', 'AMERICAN SAMOA', 'FRENCH POLYNESIA', 'NEW CALEDONIA',
            'GUAM', 'NORTHERN MARIANA ISLANDS', 'PITCAIRN', 'WALLIS AND FUTUNA'
        ]

        df_cleaned = df_cleaned.withColumn(
            "who_region",
            F.when(F.col("who_region").isNull() & F.col("country").isin(americas_countries), "Americas")
             .when(F.col("who_region").isNull() & F.col("country").isin(europe_countries), "Europe")
             .when(F.col("who_region").isNull() & F.col("country").isin(asia_countries), "Asia")
             .when(F.col("who_region").isNull() & F.col("country").isin(africa_countries), "Africa")
             .when(F.col("who_region").isNull() & F.col("country").isin(oceania_countries), "Oceania")
             .otherwise(F.col("who_region"))
        )

    # 9.  إعادة تسمية who_region إلى region_code
    # ==========================================
    if 'who_region' in df_cleaned.columns:
        df_cleaned = df_cleaned.withColumnRenamed('who_region', 'region_code')

    
    # 10.  إضافة أعمدة مساعدة (مشتقة من البيانات الموجودة)
    # ==========================================
    df_cleaned = df_cleaned.withColumn('cleaning_timestamp', F.current_timestamp())
    df_cleaned = df_cleaned.withColumn('outbreak_count', F.lit(1))

    # 11.  فحص نهائي: التأكد من عدم وجود أي قيمة فارغة
    # ==========================================
    final_count = df_cleaned.count()
    total_remaining_nulls = 0
    
    for col_name in df_cleaned.columns:
        null_count = df_cleaned.filter(
            F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")
        ).count()
        if null_count > 0:
            logger.warning(f" {col_name}: {null_count} NULL/empty values found - removing...")
            df_cleaned = df_cleaned.filter(
                F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != "")
            )
            total_remaining_nulls += null_count
    
    if total_remaining_nulls > 0:
        final_count = df_cleaned.count()
        logger.info(f" Final cleanup: removed {total_remaining_nulls} more rows")

    total_dropped = initial_count - final_count

   
    # 12. كتابة البيانات
    # ==========================================
    (df_cleaned.write
     .mode('overwrite')
     .partitionBy('year')
     .parquet(OUTPUT_PATH))

    logger.info(f" Silver: {initial_count} → {final_count} rows (dropped {total_dropped})")

    # 13. المقاييس
    # ==========================================
    return Output(
        value="Silver data materialization complete",
        metadata={
            "initial_row_count": MetadataValue.int(initial_count),
            "final_row_count": MetadataValue.int(final_count),
            "total_dropped_rows": MetadataValue.int(total_dropped),
            "null_dropped": MetadataValue.int(null_dropped),
            "empty_string_dropped": MetadataValue.int(empty_dropped),
            "duplicate_dropped": MetadataValue.int(dup_dropped),
            "year_filter_dropped": MetadataValue.int(year_dropped),
            "final_cleanup_dropped": MetadataValue.int(total_remaining_nulls),
            "null_policy": MetadataValue.text("STRICT DELETE - Zero NULLs in ALL columns"),
            "default_values": MetadataValue.text("NONE - No default values used"),
            "storage_path": MetadataValue.text(OUTPUT_PATH)
        }
    )