import os
import logging
import uuid
import re
from datetime import datetime
from typing import Optional

import spacy

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField
from clickhouse_driver import Client

# استيراد الكلمات المفتاحية من الملف المنفصل
from assets.streaming.nlp_keywords import (
    DISEASE_KEYWORDS, COUNTRY_KEYWORDS,
    DISEASE_PATTERNS, COUNTRY_PATTERNS,
    DISEASE_MAPPING, COUNTRY_MAPPING, US_STATES
)

# ========================
# Logging
# ========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========================
# Config
# ========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "outbreak_alerts")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "abdullah_developer")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "MySecurePassword123")
DB_NAME = "data_warehouse_db"

# ========================
# Schema
# ========================
schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("source", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("url", StringType(), True)
])

# ========================
# 🧠 NLP Disease & Country Extractor
# ========================
class DiseaseCountryExtractor:
    """مستخرج الأمراض والدول باستخدام spaCy NLP"""
    
    def __init__(self):
        logger.info("🧠 Loading spaCy NLP model...")
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            logger.warning("⚠️ spaCy model not found. Downloading...")
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
            self.nlp = spacy.load("en_core_web_sm")
        
        self._add_custom_patterns()
        logger.info("✅ NLP model loaded successfully")
    
    def _add_custom_patterns(self):
        """إضافة أنماط الأمراض والدول إلى spaCy"""
        if "entity_ruler" in self.nlp.pipe_names:
            self.nlp.remove_pipe("entity_ruler")
        
        ruler = self.nlp.add_pipe("entity_ruler", before="ner")
        ruler.add_patterns(DISEASE_PATTERNS + COUNTRY_PATTERNS)
    
    def extract_disease(self, text: str) -> Optional[str]:
        """استخراج المرض من النص"""
        if not text:
            return None
        
        text_lower = text.lower()
        
        # البحث المباشر
        for disease in DISEASE_KEYWORDS:
            if disease in text_lower:
                logger.info(f"✅ Disease found (direct): {disease}")
                return self._normalize_disease(disease)
        
        # استخدام spaCy
        doc = self.nlp(text[:500])
        diseases = [ent.text for ent in doc.ents if ent.label_ == "DISEASE"]
        
        if diseases:
            logger.info(f"✅ Disease found (spaCy): {diseases[0]}")
            return self._normalize_disease(diseases[0])
        
        return None
    
    def extract_country(self, text: str) -> Optional[str]:
        """استخراج الدولة من النص"""
        if not text:
            return None
        
        text_lower = text.lower()
        
        # البحث المباشر
        for country in COUNTRY_KEYWORDS:
            if country in text_lower:
                logger.info(f"✅ Country found (direct): {country}")
                return self._normalize_country(country)
        
        # استخدام spaCy
        doc = self.nlp(text[:500])
        countries = [ent.text for ent in doc.ents if ent.label_ == "COUNTRY"]
        
        if countries:
            logger.info(f"✅ Country found (spaCy): {countries[0]}")
            return self._normalize_country(countries[0])
        
        # محاولة GPE
        gpes = [ent.text for ent in doc.ents if ent.label_ == "GPE"]
        if gpes:
            for gpe in gpes:
                normalized = self._normalize_country(gpe)
                if normalized:
                    logger.info(f"✅ Country found (GPE): {gpe} -> {normalized}")
                    return normalized
        
        return None
    
    def _normalize_disease(self, disease_name: str) -> str:
        """توحيد أسماء الأمراض"""
        disease_lower = disease_name.strip().lower()
        for key, value in DISEASE_MAPPING.items():
            if key.lower() == disease_lower:
                return value
        return disease_name.strip().upper()
    
    def _normalize_country(self, country_name: str) -> Optional[str]:
        """توحيد أسماء الدول"""
        country_upper = country_name.strip().upper()
        
        if country_upper in US_STATES:
            return "UNITED STATES"
        
        country_lower = country_name.strip().lower()
        for key, value in COUNTRY_MAPPING.items():
            if key.lower() == country_lower:
                return value
        
        return country_upper

# ========================
# Initialize
# ========================
extractor = DiseaseCountryExtractor()

# ========================
# Helper functions
# ========================
def normalize(text: str) -> str:
    if not text:
        return ""
    text = text.upper()
    text = re.sub(r"[^A-Z0-9\s'\-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def parse_date(d):
    try:
        if not d:
            return datetime.utcnow()
        d = d.replace("T", " ").replace("Z", "+00:00")
        return datetime.fromisoformat(d).replace(tzinfo=None) if "+" in d else datetime.fromisoformat(d)
    except:
        return datetime.utcnow()

def get_client():
    return Client(host=CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=DB_NAME)

disease_cache = {}
location_cache = {}

def load_caches(client):
    global disease_cache, location_cache
    if not disease_cache:
        res = client.execute("SELECT disease_name, disease_key FROM dim_disease WHERE is_current = 1")
        disease_cache = {normalize(r[0]): r[1] for r in res}
        logger.info(f"✅ Loaded {len(disease_cache)} diseases")
    if not location_cache:
        res = client.execute("SELECT country, location_key FROM dim_location")
        location_cache = {normalize(r[0]): r[1] for r in res}
        logger.info(f"✅ Loaded {len(location_cache)} locations")

def write_to_clickhouse(batch_df, batch_id):
    logger.info(f"🔥 Batch {batch_id} started")
    unique_df = batch_df.dropDuplicates(["title", "source"])
    rows = unique_df.collect()
    logger.info(f"📦 Batch {batch_id} unique rows: {len(rows)}")
    if not rows:
        return
    
    client = get_client()
    load_caches(client)
    
    data = []
    stats = {"total": len(rows), "no_disease": 0, "no_country": 0, "no_keys": 0, "inserted": 0}
    
    for r in rows:
        text = f"{r.title or ''} {r.description or ''}"
        disease_name = extractor.extract_disease(text)
        country_name = extractor.extract_country(text)
        
        if not disease_name:
            stats["no_disease"] += 1
            continue
        if not country_name:
            stats["no_country"] += 1
            continue
        
        disease_key = disease_cache.get(normalize(disease_name))
        location_key = location_cache.get(normalize(country_name))
        
        if not disease_key or not location_key:
            stats["no_keys"] += 1
            logger.warning(f"⚠️ Key not found - Disease: {disease_name}, Country: {country_name}")
            continue
        
        data.append((
            str(uuid.uuid4()), disease_key, location_key,
            parse_date(r.published_at), datetime.utcnow(),
            r.title, r.source, r.url
        ))
    
    if data:
        client.execute("""
            INSERT INTO fact_outbreak_alerts
            (alert_id, disease_key, location_key, published_at, ingestion_time, title, source, url)
            VALUES
        """, data)
        stats["inserted"] = len(data)
        logger.info(f"✅ Batch {batch_id}: inserted {len(data)} rows")
    else:
        logger.warning(f"⚠️ Batch {batch_id}: no valid rows")
    
    logger.info(f"📊 Stats: total={stats['total']}, no_disease={stats['no_disease']}, no_country={stats['no_country']}, no_keys={stats['no_keys']}, inserted={stats['inserted']}")

def main():
    logger.info("🚀 Starting Spark Outbreak Consumer")
    
    spark = (SparkSession.builder.appName("OutbreakStreaming-NLP")
        .config("spark.jars",
            "/shared_jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,"
            "/shared_jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar,"
            "/shared_jars/kafka-clients-3.5.1.jar,"
            "/shared_jars/commons-pool2-2.11.1.jar")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    
    kafka_df = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load())
    
    parsed = kafka_df.selectExpr("CAST(value AS STRING) as json").select(from_json("json", schema).alias("data")).select("data.*")
    
    query = (parsed.writeStream
        .foreachBatch(write_to_clickhouse)
        .option("checkpointLocation", "/opt/spark/checkpoints/outbreak_nlp_v5")
        .trigger(processingTime="10 seconds")
        .start())
    
    query.awaitTermination()

if __name__ == "__main__":
    main()