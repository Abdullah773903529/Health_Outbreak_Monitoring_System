# dagster_code/assets/batch/bronze.py
from dagster import asset, Output
from pyspark.sql.functions import col, trim, input_file_name, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from .utils import get_spark_session

@asset(
    group_name="batch_pipeline", 
    description="استخراج بيانات الأوبئة الخام من CSV وتحويلها إلى Parquet في الطبقة البرونزية"
)
def bronze_raw_outbreak_data():
    # 1. إعداد جلسة Spark باستخدام دالة utils (لتوحيد الإعدادات وفصل المهام)
    spark = get_spark_session("Bronze_Layer")
    
    INPUT_PATTERN = "s3a://outbreak-data/*.csv"
    OUTPUT_PATH = "s3a://outbreak-data/bronze/outbreaks_parquet"

    try:
        # 2. تعريف المخطط (Schema) - نفس البيانات بدقة
        schema = StructType([
            StructField("id_outbreak", StringType(), True),
            StructField("Year", IntegerType(), True),
            StructField("icd10n", StringType(), True),
            StructField("icd103n", StringType(), True),
            StructField("icd104n", StringType(), True),
            StructField("icd10c", StringType(), True),
            StructField("icd103c", StringType(), True),
            StructField("icd104c", StringType(), True),
            StructField("Disease", StringType(), True),
            StructField("Definition", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("iso2", StringType(), True),
            StructField("iso3", StringType(), True),
            StructField("unsd_region", StringType(), True),
            StructField("unsd_subregion", StringType(), True),
            StructField("who_region", StringType(), True),
            StructField("DONs", StringType(), True)
        ])

        # 3. قراءة البيانات
        df = spark.read.option("header", True).schema(schema).csv(INPUT_PATTERN)
        df = df.withColumn("source_file", input_file_name()).withColumn("ingestion_date", current_date())

        # إسقاط السجلات التي تفتقد لأعمدة حيوية
        critical = ["id_outbreak", "Year", "Disease", "Country", "iso3"]
        df = df.na.drop(subset=critical)

        # 4. التحويل وإعادة التسمية للطبقة البرونزية
        bronze = df.select(
            trim(col("id_outbreak")).alias("outbreak_id"),
            col("Year").alias("year"),
            trim(col("Disease")).alias("disease_name"),
            trim(col("Definition")).alias("definition"),
            trim(col("icd10c")).alias("icd10_general"),
            trim(col("icd104c")).alias("icd104_specific"),
            trim(col("Country")).alias("country"),
            trim(col("iso3")).alias("iso3"),
            trim(col("who_region")).alias("who_region"),
            trim(col("unsd_region")).alias("unsd_region"),
            trim(col("unsd_subregion")).alias("unsd_subregion"),
            trim(col("DONs")).alias("don_id"),
            col("source_file"),
            col("ingestion_date")
        )

        # 5. الكتابة إلى مسار الإخراج
        bronze.write \
            .mode("overwrite") \
            .partitionBy("year") \
            .parquet(OUTPUT_PATH)
        
        # استخراج عدد السجلات لطباعتها كـ Metadata في واجهة Dagster
        rows_count = bronze.count()

        # 6. إرجاع النتيجة لـ Dagster
        return Output(
            value=OUTPUT_PATH, 
            metadata={
                "rows_count": rows_count, 
                "layer": "Bronze", 
                "format": "Parquet",
                "partitions": "year"
            }
        )

    except Exception as e:
        # التقاط الأخطاء وتمريرها ليتم تسجيلها في Dagster
        raise Exception(f"An error occurred during Bronze ETL process: {e}")

    finally:
        # تم إيقاف هذا السطر للسماح للطبقة الفضية باستكمال العمل على نفس الجلسة
        # spark.stop()
        pass