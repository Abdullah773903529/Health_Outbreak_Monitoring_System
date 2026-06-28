from dagster import asset, Output, MetadataValue
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from .utils import get_spark_session


@asset(
    deps=["silver_cleaned_outbreaks"],
    compute_kind="spark",
    group_name="batch_layer"
)
def gold_data_warehouse_load(context):

    spark = get_spark_session("GoldLoadingJob")

    # =====================================================
    # إعدادات القراءة والاتصال
    # =====================================================

    SILVER_INPUT_PATH = "s3a://outbreak-data/silver/outbreaks_cleaned_parquet"

    DB_URL = "jdbc:postgresql://postgres-dw:5432/data_warehouse_db"

    DB_PROPERTIES = {
        "user": "abdullah_developer",
        "password": "MySecurePassword123",
        "driver": "org.postgresql.Driver"
    }

    # =====================================================
    # قراءة بيانات Silver
    # =====================================================

    df_silver = spark.read.parquet(SILVER_INPUT_PATH)

    context.log.info(f"Silver records: {df_silver.count()}")

    # =====================================================
    # DIM_LOCATION
    # =====================================================

    dim_location = (
        df_silver
        .select(
            "iso3",
            "country",
            "region_code",
            "unsd_region",
            "unsd_subregion"
        )
        .dropDuplicates(["iso3"])
        .withColumn("location_key", F.md5(F.col("iso3")))
        .select(
            "location_key",
            "iso3",
            "country",
            "region_code",
            "unsd_region",
            "unsd_subregion"
        )
    )

    # =====================================================
    # DIM_DISEASE
    # =====================================================

    dim_disease = (
        df_silver
        .select(
            "disease_name",
            "definition",
            "icd10_general",
            "icd104_specific"
        )
        .dropDuplicates(["disease_name"])
        .withColumn(
            "disease_key",
            F.abs(F.hash("disease_name")).cast(IntegerType())
        )
        .withColumn("start_date", F.current_date())
        .withColumn("end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .select(
            "disease_key",
            "disease_name",
            "definition",
            "icd10_general",
            "icd104_specific",
            "start_date",
            "end_date",
            "is_current"
        )
    )

    # =====================================================
    # FACT_OUTBREAKS
    # =====================================================

    fact_outbreaks = (
        df_silver.alias("s")
        .join(
            dim_location.alias("l"),
            F.col("s.iso3") == F.col("l.iso3"),
            "left"
        )
        .join(
            dim_disease.alias("d"),
            F.col("s.disease_name") == F.col("d.disease_name"),
            "left"
        )
        .select(
            F.col("s.outbreak_id"),
            F.col("l.location_key"),
            F.col("d.disease_key"),
            F.col("s.year").alias("report_year"),
            F.col("s.don_id"),
            F.col("s.outbreak_count"),
            F.current_timestamp().alias("ingestion_timestamp")
        )
    )

    # =====================================================
    # تحويل DON_ID إلى صفوف منفصلة
    # =====================================================

    fact_outbreaks = (
        fact_outbreaks
        .withColumn(
            "don_id",
            F.explode(F.split(F.col("don_id"), ","))
        )
        .withColumn(
            "don_id",
            F.trim(F.col("don_id"))
        )
        .filter(
            (F.col("don_id").isNotNull()) &
            (F.col("don_id") != "")
        )
    )

    # =====================================================
    # إزالة أي صفوف مكررة
    # =====================================================

    fact_outbreaks = fact_outbreaks.dropDuplicates(
        ["outbreak_id", "don_id"]
    )

    # =====================================================
    # فحص التكرارات (للتأكد فقط)
    # =====================================================

    duplicate_count = (
        fact_outbreaks
        .groupBy("outbreak_id", "don_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    context.log.info(f"Duplicate rows after cleanup: {duplicate_count}")

    # =====================================================
    # الإحصائيات
    # =====================================================

    loc_count = dim_location.count()
    dis_count = dim_disease.count()
    fact_count = fact_outbreaks.count()

    context.log.info(f"Locations : {loc_count}")
    context.log.info(f"Diseases : {dis_count}")
    context.log.info(f"Facts : {fact_count}")

    # =====================================================
    # الكتابة إلى PostgreSQL
    # =====================================================

    dim_location.write.jdbc(
        url=DB_URL,
        table="dim_location",
        mode="append",
        properties=DB_PROPERTIES
    )

    dim_disease.write.jdbc(
        url=DB_URL,
        table="dim_disease",
        mode="append",
        properties=DB_PROPERTIES
    )

    fact_outbreaks.write.jdbc(
        url=DB_URL,
        table="fact_outbreaks",
        mode="append",
        properties=DB_PROPERTIES
    )

    context.log.info("Gold Layer Loaded Successfully")

    return Output(
        value="Gold Layer loaded successfully",
        metadata={
            "dim_location_inserted": MetadataValue.int(loc_count),
            "dim_disease_inserted": MetadataValue.int(dis_count),
            "fact_outbreaks_inserted": MetadataValue.int(fact_count),
            "database_target": MetadataValue.text(
                "postgres-dw:data_warehouse_db"
            )
        }
    )