from dagster import asset, Output, MetadataValue
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, sum as spark_sum, count, avg, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

@asset(deps=["generate_transactions"])
def spark_fraud_detection():
    """
    قراءة المعاملات من Kafka باستخدام Spark (Local Mode)
    """
    print("=" * 60)
    print("🔍 بدء تحليل المعاملات باستخدام Spark...")
    print("=" * 60)
    
    # إنشاء Spark Session مع جميع الإعدادات المطلوبة
    spark = SparkSession.builder \
        .appName("FraudDetection") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.jars", "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # مخطط البيانات القادمة من Kafka
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("country", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("is_fraud", IntegerType(), True)
    ])
    
    try:
        # 1. قراءة البيانات من Kafka
        print("📤 جاري قراءة البيانات من Kafka...")
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "transactions") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # 2. تحويل JSON إلى DataFrame
        print("📤 جاري تحويل البيانات...")
        parsed_df = df \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")
        
        # 3. حساب الإحصائيات
        print("📤 جاري حساب الإحصائيات...")
        transaction_stats = parsed_df \
            .groupBy("currency") \
            .agg(
                count("*").alias("transaction_count"),
                spark_sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                spark_max("amount").alias("max_amount"),
                spark_sum(when(col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count")
            )
        
        stats_count = transaction_stats.count()
        
        if stats_count == 0:
            print("⚠️ لا توجد بيانات في Kafka!")
            return Output(
                value={"status": "No data"},
                metadata={"status": "No data in Kafka"}
            )
        
        # 4. عرض النتائج
        print(f"📊 تم حساب الإحصائيات لـ {stats_count} عملات")
        transaction_stats.show(truncate=False)
        
        # 5. حفظ في PostgreSQL
        print("📤 جاري حفظ البيانات في PostgreSQL...")
        transaction_stats.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres-dw:5432/data_warehouse_db") \
            .option("dbtable", "transaction_stats") \
            .option("user", "abdullah_developer") \
            .option("password", "MySecurePassword123") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        print(f"✅ تم حفظ {stats_count} صف في PostgreSQL")
        
        # 6. تحويل DataFrame إلى قائمة (List) لإرجاعها (بدلاً من DataFrame)
        result_data = transaction_stats.collect()
        result_list = []
        for row in result_data:
            result_list.append({
                "currency": row["currency"],
                "transaction_count": row["transaction_count"],
                "total_amount": float(row["total_amount"]) if row["total_amount"] else 0.0,
                "avg_amount": float(row["avg_amount"]) if row["avg_amount"] else 0.0,
                "max_amount": float(row["max_amount"]) if row["max_amount"] else 0.0,
                "fraud_count": row["fraud_count"]
            })
        
        print("=" * 60)
        
        return Output(
            value={
                "status": "success",
                "data": result_list,
                "row_count": stats_count
            },
            metadata={
                "processed_rows": stats_count,
                "table": "transaction_stats",
                "currencies": stats_count,
                "preview": str(result_list[:3])
            }
        )
        
    except Exception as e:
        print(f"❌ خطأ: {e}")
        import traceback
        traceback.print_exc()
        return Output(
            value={"status": "error", "error": str(e)},
            metadata={"error": str(e)}
        )