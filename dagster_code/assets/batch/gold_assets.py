from dagster import asset, get_dagster_logger, Output, MetadataValue
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import DateType
from .utils import get_spark_session
import requests
import json


@asset(
    deps=["silver_cleaned_outbreaks"],
    group_name="gold_layer",
    description="Production Gold Layer (Atomic + SCD2 + Broadcast + HTTP)"
)
def load_gold_data_warehouse(context):

    logger = get_dagster_logger()

    # ==========================================
    # 1. Spark Session
    # ==========================================
    spark = get_spark_session("Gold_Layer_Atomic_Production")

    # ==========================================
    # 2. Read Silver
    # ==========================================
    silver_path = "s3a://outbreak-data/silver/outbreaks_cleaned_parquet"
    silver_df = spark.read.parquet(silver_path)

    logger.info(f"Silver loaded: {silver_df.count()} rows")

    # ==========================================
    # 3. ClickHouse Config
    # ==========================================
    CLICKHOUSE_URL = "http://clickhouse:8123/"
    DB = "data_warehouse_db"
    USER = "abdullah_developer"
    PASSWORD = "MySecurePassword123"

    # ==========================================
    # 4. DIM_LOCATION
    # ==========================================
    dim_location = (
        silver_df
        .select("iso3", "country", "region_code", "unsd_region", "unsd_subregion")
        .dropDuplicates(["iso3"])
        .dropna(subset=["iso3"])
        .withColumn("location_key", F.md5(F.col("iso3")))
    )

    # ==========================================
    # 5. DIM_DISEASE (SCD2 PREP)
    # ==========================================
    silver_disease = (
        silver_df
        .select("disease_name", "definition", "icd10_general", "icd104_specific")
        .dropDuplicates(["disease_name"])
        .dropna(subset=["disease_name"])
    )

    disease_names = [r["disease_name"] for r in silver_disease.collect()]

    dim_disease = (
        silver_disease
        .withColumn(
            "disease_key",
            F.md5(F.concat_ws("_", F.col("disease_name"), F.current_timestamp().cast("string")))
        )
        .withColumn("start_date", F.current_date())
        .withColumn("end_date", F.lit(None).cast(DateType()))
        .withColumn("is_current", F.lit(1))
    )

    # ==========================================
    # 6. FACT TABLE
    # ==========================================
    fact_outbreaks = (
        silver_df.alias("s")
        .join(broadcast(dim_location.alias("l")), "iso3", "inner")
        .join(broadcast(dim_disease.alias("d")), "disease_name", "inner")
        .select(
            F.col("l.location_key"),
            F.col("d.disease_key"),
            F.col("s.year").alias("report_year"),
            F.col("s.don_id"),
            F.lit(1).alias("outbreak_count")
        )
        .withColumn(
            "outbreak_id",
            F.md5(F.concat_ws("_",
                F.col("location_key"),
                F.col("disease_key"),
                F.col("don_id"),
                F.col("report_year")
            ))
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .dropDuplicates(["outbreak_id"])
    )

    # ==========================================
    # 7. ✅✅✅ WRITER الآمن - يستخدم collect() فقط ✅✅✅
    # ==========================================
    def write_to_clickhouse(df, table_name):
        """كتابة آمنة بدون foreachPartition"""
        logger.info(f"📤 Collecting data for {table_name}...")
        
        # ✅ نجمع كل البيانات كـ list of dicts إلى الـ Driver
        rows = df.collect()
        logger.info(f"📊 Collected {len(rows)} rows for {table_name}")
        
        if not rows:
            logger.warning(f"⚠️ No data to write to {table_name}")
            return
        
        # ✅ نحول الـ Rows إلى JSON strings
        json_lines = []
        for row in rows:
            row_dict = row.asDict()
            json_line = json.dumps(row_dict, default=str)
            json_lines.append(json_line)
        
        # ✅ نرسل على دفعات
        BATCH_SIZE = 5000
        total = len(json_lines)
        
        for i in range(0, total, BATCH_SIZE):
            batch = json_lines[i:i+BATCH_SIZE]
            batch_num = i//BATCH_SIZE + 1
            total_batches = (total + BATCH_SIZE - 1)//BATCH_SIZE
            
            try:
                response = requests.post(
                    CLICKHOUSE_URL,
                    params={"query": f"INSERT INTO {DB}.{table_name} FORMAT JSONEachRow"},
                    data="\n".join(batch).encode("utf-8"),
                    auth=(USER, PASSWORD),
                    headers={"Content-Type": "text/plain"},
                    timeout=60
                )
                
                if response.status_code == 200:
                    logger.info(f"✅ Batch {batch_num}/{total_batches} done ({len(batch)} rows)")
                else:
                    raise Exception(f"HTTP {response.status_code}: {response.text}")
                    
            except Exception as e:
                logger.error(f"❌ Failed batch {batch_num}: {str(e)}")
                raise
        
        logger.info(f"✅ All {total} rows written to {table_name}")

    # ==========================================
    # 8. SCD2 EXPIRATION
    # ==========================================
    def expire_old_diseases(names):
        if not names:
            return

        for i in range(0, len(names), 500):
            chunk = names[i:i+500]
            names_sql = ",".join([f"'{n}'" for n in chunk])

            query = f"""
            ALTER TABLE {DB}.dim_disease
            UPDATE
                is_current = 0,
                end_date = today()
            WHERE disease_name IN ({names_sql})
            AND is_current = 1
            """

            requests.post(
                CLICKHOUSE_URL,
                params={"query": query},
                auth=(USER, PASSWORD)
            )

    # ==========================================
    # 9. EXECUTION
    # ==========================================
    try:
        logger.info("Expiring old diseases (SCD2)...")
        expire_old_diseases(disease_names)

        logger.info("Writing dim_location...")
        write_to_clickhouse(dim_location, "dim_location")

        logger.info("Writing dim_disease...")
        write_to_clickhouse(dim_disease, "dim_disease")

        logger.info("Writing fact_outbreaks...")
        write_to_clickhouse(fact_outbreaks, "fact_outbreaks")

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {str(e)}")
        raise

    # ==========================================
    # 10. METADATA
    # ==========================================
    return Output(
        value="SUCCESS",
        metadata={
            "dim_location": MetadataValue.int(dim_location.count()),
            "dim_disease": MetadataValue.int(dim_disease.count()),
            "fact_outbreaks": MetadataValue.int(fact_outbreaks.count()),
            "write_mode": MetadataValue.text("collect() - safe mode"),
            "status": MetadataValue.text("FIXED")
        }
    )