# dagster_code/assets/batch/utils.py
import os
from pyspark.sql import SparkSession

def get_spark_session(app_name="Dagster_Spark_Job"):
    MINIO_ENDPOINT = "http://minio:9000" 
    SPARK_MASTER = "spark://spark-master:7077"

    spark = (SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.driver.host", "dagster-webserver")
        # تحديد الذاكرة بوضوح ليتناسب مع ما يراه الـ Worker
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        
        # إعدادات الاتصال بـ MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate())
    
    return spark