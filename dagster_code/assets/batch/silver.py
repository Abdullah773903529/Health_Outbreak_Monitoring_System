from dagster import asset, Output, MetadataValue
from pyspark.sql import functions as F
from .utils import get_spark_session

@asset(
    deps=["bronze_raw_outbreak_data"],  # الاعتمادية على أصل الطبقة البرونزية
    compute_kind="spark",
    group_name="batch_layer"
)
def silver_cleaned_outbreaks():
    """
    الطبقة الفضية (Silver Layer):
    تنظيف البيانات الخام، توحيد حالة الحروف والنصوص، معالجة القيم المفقودة (Nulls)،
    وإعادة هيكلة البيانات لتكون جاهزة للمرحلة الثالثة (Gold/Database).
    """
    # 1. جلب جلسة Spark الموحدة من ملف utils
    spark = get_spark_session("SilverCleaningJob")
    
    INPUT_PATH = 's3a://outbreak-data/bronze/outbreaks_parquet'
    OUTPUT_PATH = 's3a://outbreak-data/silver/outbreaks_cleaned_parquet'
    
    # 2. قراءة البيانات البرونزية المخزنة بصيغة Parquet
    df = spark.read.parquet(INPUT_PATH)
    
    # تسجيل عدد السجلات قبل التنظيف لحساب مقاييس الجودة
    initial_count = df.count()
    
    # 3. خطوة التنظيف الصارم وإزالة المكررات (Data Cleansing)
    critical_columns = ['outbreak_id', 'year', 'disease_name', 'country', 'iso3']
    
    df_cleaned = (
        df.na.drop(subset=critical_columns)
          .dropDuplicates(['outbreak_id'])
          .filter((F.col('year') >= 1996) & (F.col('year') <= 2100))
    )
    
    # 4. أتمتة معالجة النصوص وتوحيدها (تحسين قابلية التوسع)
    # أعمدة تحتاج تحويل للحروف الكبيرة وإزالة الفراغات
    upper_trim_cols = [
        "disease_name", "country", "iso3", "who_region", 
        "unsd_region", "unsd_subregion", "icd10_general"
    ]
    for col_name in upper_trim_cols:
        if col_name in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(col_name, F.trim(F.upper(F.col(col_name))))
            
    # أعمدة تحتاج إزالة الفراغات فقط (بدون تكبير الحروف مثل النصوص الطويلة)
    trim_only_cols = ["definition", "don_id"]
    for col_name in trim_only_cols:
        if col_name in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(col_name, F.trim(F.col(col_name)))

    # 5. معالجة القيم الفارغة الخاصة وتغيير أسماء الأعمدة المعيارية
    df_cleaned = (
        df_cleaned
        .withColumn(
            'icd104_specific',
            F.when(F.col('icd104_specific').isNull(), 'UNKNOWN')
             .otherwise(F.upper(F.trim(F.col('icd104_specific'))))
        )
        .withColumnRenamed('who_region', 'region_code') # إعادة التسمية حسب معيارك السابق
        .withColumn('cleaning_timestamp', F.current_timestamp()) # وقت التنظيف
        .withColumn('outbreak_count', F.lit(1)) # عمود تجميعي للمستقبل
    )
    
    # حساب عدد السجلات بعد التنظيف
    final_count = df_cleaned.count()
    dropped_rows = initial_count - final_count

    # 6. كتابة البيانات مقسمة حسب السنة (Partitioned by Year) لتحسين الأداء
    (df_cleaned.write
     .mode('overwrite')
     .partitionBy('year')
     .parquet(OUTPUT_PATH))
    
    # 7. تمرير المقاييس الذكية لواجهة Dagster لتبدو كمهندس بيانات محترف
    return Output(
        value="Silver data materialization complete",
        metadata={
            "initial_row_count": MetadataValue.int(initial_count),
            "final_row_count": MetadataValue.int(final_count),
            "dropped_rows_count": MetadataValue.int(dropped_rows),
            "storage_path": MetadataValue.text(OUTPUT_PATH)
        }
    )