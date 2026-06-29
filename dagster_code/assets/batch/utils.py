import os
from pyspark.sql import SparkSession

def get_spark_session(app_name="Dagster_Spark_Job"):
    MINIO_ENDPOINT = "http://minio:9000" 
    SPARK_MASTER = "spark://spark-master:7077"
    
    # تعريف المسارات الموحدة للملفات الجديدة (تم استبدال درايفر Postgres بدرايفر ClickHouse الجديد)
    JARS = [
        "/shared_jars/clickhouse-jdbc-0.6.4-all.jar", # البطل الجديد هنا 🚀
        "/shared_jars/hadoop-aws-3.3.4.jar",
        "/shared_jars/aws-java-sdk-bundle-1.12.262.jar"
    ]
    jars_string = ",".join(JARS)
    classpath_string = ":".join(JARS)

    spark = (SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.driver.host", "dagster-webserver")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        
        # إرسال المكتبات لجميع العقد (Workers/Driver)
        .config("spark.jars", jars_string)
        
        # إجبار الـ Driver والـ Executors على رؤية الملفات في الـ ClassPath
        .config("spark.driver.extraClassPath", classpath_string)
        .config("spark.executor.extraClassPath", classpath_string)
        
        # إزالة spark.jars.packages تماماً لأننا نوفر الملفات يدوياً الآن
        
        # إعدادات الاتصال بـ MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate())
    
    return spark