from dagster import asset, Output, MetadataValue
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from .utils import get_spark_session
import requests
import json
import pandas as pd


@asset(
    deps=["silver_cleaned_outbreaks"],
    compute_kind="spark",
    group_name="batch_layer"
)
def gold_data_warehouse_load(context):

    spark = get_spark_session("GoldLoadingJob")

    # =====================================================
    # إعدادات القراءة والاتصال بـ ClickHouse
    # =====================================================

    SILVER_INPUT_PATH = "s3a://outbreak-data/silver/outbreaks_cleaned_parquet"
    
    # استخدام HTTP بدلاً من JDBC
    CLICKHOUSE_HTTP_URL = "http://clickhouse:8123/"
    CLICKHOUSE_USER = "abdullah_developer"
    CLICKHOUSE_PASSWORD = "MySecurePassword123"
    CLICKHOUSE_DATABASE = "data_warehouse_db"

    # =====================================================
    # قراءة بيانات Silver
    # =====================================================

    df_silver = spark.read.parquet(SILVER_INPUT_PATH)
    context.log.info(f"📊 Silver records loaded: {df_silver.count()}")

    # =====================================================
    # DIM_LOCATION
    # =====================================================

    dim_location = (
        df_silver
        .select("iso3", "country", "region_code", "unsd_region", "unsd_subregion")
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
        .select("disease_name", "definition", "icd10_general", "icd104_specific")
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
    # إزالة التكرارات
    # =====================================================

    fact_outbreaks = fact_outbreaks.dropDuplicates(["outbreak_id", "don_id"])

    # =====================================================
    # الإحصائيات
    # =====================================================

    loc_count = dim_location.count()
    dis_count = dim_disease.count()
    fact_count = fact_outbreaks.count()

    context.log.info(f"📍 Locations to insert: {loc_count}")
    context.log.info(f"🦠 Diseases to insert: {dis_count}")
    context.log.info(f"📋 Facts to insert: {fact_count}")

    # =====================================================
    # دالة الكتابة عبر HTTP API
    # =====================================================

    def write_to_clickhouse_http(df, table_name, batch_size=5000):
        """
        كتابة DataFrame إلى ClickHouse عبر HTTP API
        هذه الطريقة تتجاوز مشكلة CREATE TABLE تماماً
        """
        # تحويل DataFrame إلى pandas
        pandas_df = df.toPandas()
        total_rows = len(pandas_df)
        
        context.log.info(f"📤 Writing {total_rows} rows to {table_name}...")
        
        # تقسيم البيانات إلى batches للكتابة الفعالة
        rows_inserted = 0
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = pandas_df.iloc[start_idx:end_idx]
            
            # تحويل الـ batch إلى JSONEachRow format
            records = batch_df.to_dict('records')
            
            # معالجة القيم الخاصة (None, timestamps, etc.)
            json_lines = []
            for record in records:
                cleaned_record = {}
                for key, value in record.items():
                    if value is None:
                        cleaned_record[key] = None
                    elif isinstance(value, pd.Timestamp):
                        cleaned_record[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                    elif hasattr(value, 'item'):  # numpy types
                        cleaned_record[key] = value.item()
                    else:
                        cleaned_record[key] = value
                
                json_lines.append(json.dumps(cleaned_record, default=str))
            
            # إرسال البيانات
            data = '\n'.join(json_lines)
            
            query_params = {
                "query": f"INSERT INTO {CLICKHOUSE_DATABASE}.{table_name} FORMAT JSONEachRow"
            }
            
            response = requests.post(
                CLICKHOUSE_HTTP_URL,
                params=query_params,
                data=data.encode('utf-8'),
                auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
                headers={'Content-Type': 'text/plain; charset=utf-8'}
            )
            
            if response.status_code != 200:
                error_msg = f"HTTP {response.status_code}: {response.text[:500]}"
                raise Exception(f"Failed to insert into {table_name}: {error_msg}")
            
            rows_inserted += len(batch_df)
            
            # تسجيل التقدم كل 10000 صف
            if rows_inserted % 10000 == 0 or rows_inserted == total_rows:
                context.log.info(f"⏳ {table_name}: {rows_inserted}/{total_rows} rows inserted")
        
        context.log.info(f"✅ {table_name}: {rows_inserted} rows inserted successfully")
        return rows_inserted

    # =====================================================
    # اختبار الاتصال بـ ClickHouse أولاً
    # =====================================================

    try:
        test_response = requests.get(
            CLICKHOUSE_HTTP_URL,
            auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
        )
        if test_response.status_code == 200:
            context.log.info("🔗 ClickHouse connection test: OK")
        else:
            raise Exception(f"Connection test failed: HTTP {test_response.status_code}")
    except Exception as e:
        context.log.error(f"❌ Cannot connect to ClickHouse: {str(e)}")
        raise

    # =====================================================
    # تنفيذ الكتابة
    # =====================================================

    try:
        context.log.info("📤 Writing dim_location...")
        written_loc = write_to_clickhouse_http(dim_location, "dim_location")
        context.log.info(f"✅ dim_location: {written_loc} rows written")
    except Exception as e:
        context.log.error(f"❌ Failed to write dim_location: {str(e)}")
        raise

    try:
        context.log.info("📤 Writing dim_disease...")
        written_dis = write_to_clickhouse_http(dim_disease, "dim_disease")
        context.log.info(f"✅ dim_disease: {written_dis} rows written")
    except Exception as e:
        context.log.error(f"❌ Failed to write dim_disease: {str(e)}")
        raise

    try:
        context.log.info("📤 Writing fact_outbreaks...")
        written_fact = write_to_clickhouse_http(fact_outbreaks, "fact_outbreaks")
        context.log.info(f"✅ fact_outbreaks: {written_fact} rows written")
    except Exception as e:
        context.log.error(f"❌ Failed to write fact_outbreaks: {str(e)}")
        raise

    # =====================================================
    # النجاح
    # =====================================================

    context.log.info("🎉 Gold Layer Loaded Successfully via HTTP API")

    return Output(
        value="Gold Layer loaded successfully into ClickHouse via HTTP",
        metadata={
            "dim_location_inserted": MetadataValue.int(loc_count),
            "dim_disease_inserted": MetadataValue.int(dis_count),
            "fact_outbreaks_inserted": MetadataValue.int(fact_count),
            "database_target": MetadataValue.text(f"clickhouse://{CLICKHOUSE_DATABASE}"),
            "write_method": MetadataValue.text("HTTP API"),
            "write_mode": MetadataValue.text("append")
        }
    )