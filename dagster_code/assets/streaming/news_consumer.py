import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, StructField
from clickhouse_driver import Client
from datetime import datetime
import uuid 

# ==========================================================
# 1. الإعدادات العامة (Configuration & Logging)
# ==========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "outbreak_alerts")
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "abdullah_developer")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "MySecurePassword123")

# ==========================================================
# 2. القواميس الذكية للاستخراج (Mappings)
# ==========================================================
DISEASE_MAPPING = {
    "ebola": "Ebola", "cholera": "Cholera", "covid": "COVID-19", 
    "mpox": "Mpox", "monkeypox": "Mpox", "bird flu": "Bird Flu", 
    "avian influenza": "Bird Flu", "h5n1": "Bird Flu", "malaria": "Malaria", 
    "dengue": "Dengue", "polio": "Polio", "zika": "Zika", 
    "measles": "Measles", "influenza": "Influenza", "flu": "Influenza", 
    "hantavirus": "Hantavirus", "west nile": "West Nile Virus", "screwworm": "Screwworm"
}

COUNTRY_MAPPING = {
    "congo": "Democratic Republic of the Congo",
    "drc": "Democratic Republic of the Congo",
    "uganda": "Uganda",
    "oklahoma": "United States",
    "texas": "United States",
    "lancaster": "United States",
    "us ": "United States",
    "usa": "United States",
    "united states": "United States",
    "nigeria": "Nigeria",
    "nepal": "Nepal",
    "kathmandu": "Nepal",
    "central african republic": "Central African Republic",
    "queensland": "Australia",
    "australia": "Australia",
    "united kingdom": "United Kingdom",
    "uk": "United Kingdom"
}

# ==========================================================
# 3. دوال الاستخراج المخصصة (UDFs)
# ==========================================================
def extract_disease_name(title: str, description: str) -> str:
    text = f"{title or ''} {description or ''}".lower()
    for keyword, std_name in DISEASE_MAPPING.items():
        if keyword in text:
            return std_name
    return "Other Infectious Disease"

def extract_country_name(title: str, description: str) -> str:
    text = f"{title or ''} {description or ''}".lower()
    padded_text = f" {text} "
    for keyword, std_country in COUNTRY_MAPPING.items():
        if keyword in padded_text:
            return std_country
    return "Global / Unknown"

extract_disease_udf = udf(extract_disease_name, StringType())
extract_country_udf = udf(extract_country_name, StringType())

# ==========================================================
# 4. هيكل البيانات القادمة من Kafka (Schema)
# ==========================================================
schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("source", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("url", StringType(), True)
])

# ==========================================================
# 5. دالة الكتابة في ClickHouse (Micro-Batch Processing)
# ==========================================================
def parse_date(raw_date):
    """
    تحويل التاريخ القادم من Kafka إلى datetime آمن لـ ClickHouse
    """
    try:
        if not raw_date:
            return datetime.utcnow()
        # تحويل التاريخ والتأكد من إزالة الـ tzinfo لتجنب أي تعارض
        dt = datetime.fromisoformat(
            raw_date.replace("T", " ").replace("Z", "+00:00")
        )
        return dt.replace(tzinfo=None)
    except Exception:
        return datetime.utcnow()

def write_to_clickhouse(batch_df, batch_id):
    # تحويل الـ DataFrame إلى dict
    rows = [row.asDict() for row in batch_df.collect()]

    if not rows:
        return

    client = Client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database='data_warehouse_db'
    )

    prepared_data = []

    for row in rows:
        try:
            # توليد معرف فريد لكل سجل لتجنب خطأ الحقل المفقود
            alert_id = str(uuid.uuid4())
            
            record = (
                parse_date(row.get("published_at")),
                str(row.get("disease_name", "Other Infectious Disease")),
                str(row.get("country", "Global / Unknown")),
                str(row.get("title", "")),
                str(row.get("source", "")),
                str(row.get("url", "")),
                alert_id # تمت إضافة الحقل هنا
            )

            prepared_data.append(record)

        except Exception as e:
            logger.error(f"❌ خطأ في تجهيز الصف: {e}")

    # إدخال الدفعة إلى ClickHouse
    if prepared_data:
        try:
            client.execute(
                """
                INSERT INTO fact_outbreak_alerts
                (published_at, disease_name, country, title, source, url, alert_id)
                VALUES
                """,
                prepared_data
            )

            logger.info(f"📊 الدفعة {batch_id}: تم إدخال {len(prepared_data)} سجل بنجاح.")

        except Exception as e:
            logger.error(f"❌ فشل الإدخال في ClickHouse: {e}")
# ==========================================================
# 6. المحرك الرئيسي
# ==========================================================
def main():
    logger.info("🚀 بدء تشغيل الـ Spark Outbreak Consumer...")

    # تحديث مسارات المكتبات لتكون قائمة واضحة ومباشرة
    spark_jars = [
        "/shared_jars/spark-sql-kafka-0-10_2.12-3.5.5.jar",
        "/shared_jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar",
        "/shared_jars/kafka-clients-3.5.1.jar",
        "/shared_jars/commons-pool2-2.11.1.jar"
    ]
    
    # تحويل القائمة إلى نص مفصول بفاصلة
    jars_str = ",".join(spark_jars)

    spark = SparkSession.builder \
        .appName("OutbreakSparkConsumer") \
        .config("spark.jars", jars_str) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # بقية كود الـ Stream الخاص بك...
    parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json("json_str", schema).alias("data")) \
        .select("data.*")

    transformed_stream = parsed_stream \
        .withColumn("disease_name", extract_disease_udf(col("title"), col("description"))) \
        .withColumn("country", extract_country_udf(col("title"), col("description")))

    query = transformed_stream.writeStream \
        .foreachBatch(write_to_clickhouse) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()