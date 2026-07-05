import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import requests
from kafka import KafkaProducer
from dagster import asset

load_dotenv()

# ==========================================
# 1. الإعدادات العامة (Configuration)
# ==========================================
API_KEY = os.getenv("NEWS_API_KEY", "YOUR_NEWS_API_KEY")
NEWS_API_URL = "https://newsapi.org/v2/everything"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "outbreak_alerts")
REQUEST_TIMEOUT = 20

# ==========================================
# 2. الكلمات المفتاحية للفلترة
# ==========================================
INFECTIOUS_DISEASES = [
    "cholera", "ebola", "covid", "covid-19", "influenza", "flu", "bird flu", 
    "avian influenza", "west nile", "h5n1", "measles", "mpox", "monkeypox", 
    "malaria", "dengue", "polio", "zika", "tuberculosis", "virus", "infection", 
    "bacterial outbreak", "screwworm"
]
OUTBREAK_CONTEXTS = [
    "outbreak", "epidemic", "pandemic", "first case", "confirmed case", 
    "confirmed cases", "new cases", "surge", "cluster", "detected", 
    "health alert", "emergency", "spread", "transmission", "infected"
]
EXCLUDED_KEYWORDS = [
    "celebrity", "concert", "movie", "music", "sleep", "diet", "fitness", 
    "relationship", "pet", "recipe", "weight loss", "body type", "aging", 
    "wellness", "cancer", "diabetes", "heart disease", "cyber", "computer virus", 
    "software", "hacker", "malware", "mental health", "clinical trial"
]

# ==========================================
# 3. إعداد السجلات والـ Kafka Producer
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    acks="all"
)

# مجموعات لمنع التكرار داخل نفس الـ Run
seen_urls = set()
seen_titles = set()

# ==========================================
# 4. دوال المعالجة والجلب
# ==========================================
def is_relevant_outbreak(article: dict) -> bool:
    """التحقق من أن الخبر طبي ويتعلق بوباء معدي"""
    title = (article.get("title") or "").lower()
    desc = (article.get("description") or "").lower()
    full_text = f"{title} {desc}"

    if any(keyword in full_text for keyword in EXCLUDED_KEYWORDS):
        return False
    has_disease = any(disease in full_text for disease in INFECTIOUS_DISEASES)
    has_outbreak_context = any(context in full_text for context in OUTBREAK_CONTEXTS)
    return has_disease and has_outbreak_context

def fetch_news_with_retry(max_retries=3, backoff_factor=2) -> dict:
    """جلب الأخبار من NewsAPI لنطاق زمني يشمل الأمس واليوم لضمان عدم ضياع البيانات"""
    # حساب تواريخ الأمس واليوم
    today_date = datetime.now(timezone.utc).date()
    yesterday_date = today_date - timedelta(days=1)
    
    params = {
        "q": '("outbreak" OR "epidemic" OR "pandemic" OR "new cases") AND ("virus" OR "infection" OR "disease" OR "cholera" OR "ebola" OR "dengue")',
        "language": "en",
        "sortBy": "publishedAt",
        "from": yesterday_date.isoformat(),  # جلب الأخبار من الأمس
        "to": today_date.isoformat(),        # حتى اليوم
        "pageSize": 100,
        "apiKey": API_KEY
    }
    
    retries = 0
    delay = 4
    while retries < max_retries:
        try:
            with requests.Session() as session:
                response = session.get(NEWS_API_URL, params=params, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                return response.json()
        except requests.exceptions.RequestException as e:
            retries += 1
            logger.warning(f"⚠️ المحاولة {retries} فشلت. إعادة المحاولة خلال {delay} ثوانٍ...")
            if retries == max_retries:
                logger.error("❌ تم استنفاد المحاولات للاتصال بـ API.")
                raise e
            time.sleep(delay)
            delay *= backoff_factor

def process_and_send_articles(articles: list) -> int:
    """فلترة المقالات وإرسال المطابق منها إلى كافكا"""
    sent_count = 0
    for article in articles:
        title = (article.get("title") or "").strip()
        url = article.get("url")
        
        # تخطي المقالات الفارغة أو التي لا تطابق شروط الأوبئة
        if not title or not url or not is_relevant_outbreak(article):
            continue
            
        title_lower = title.lower()
        
        # تخطي المقالات المكررة في نفس التشغيلة
        if url in seen_urls or title_lower in seen_titles:
            continue
            
        seen_urls.add(url)
        seen_titles.add(title_lower)
        
        # تجهيز الرسالة
        message = {
            "title": title,
            "description": article.get("description"),
            "content": article.get("content"),
            "author": article.get("author"),
            "source": article.get("source", {}).get("name"),
            "published_at": article.get("publishedAt"),
            "url": url,
            "language": "en",
            "ingestion_time": datetime.now(timezone.utc).isoformat()
        }
        
        # إرسال إلى كافكا
        producer.send(KAFKA_TOPIC, key=title.encode("utf-8"), value=message)
        sent_count += 1
        logger.info(f"✅ تم إرسال تنبيه: {title}")
        
    producer.flush()
    return sent_count

# ==========================================
# 5. Dagster Asset
# ==========================================
@asset(group_name="streaming", compute_kind="python")
def fetch_and_produce_outbreak_news():
    """جلب التنبيهات والأخبار الطبية من API وإرسالها إلى كافكا"""
    logger.info("🚀 بدء تشغيل الـ News Outbreak Producer...")
    try:
        data = fetch_news_with_retry()
        if data.get("status") != "ok":
            logger.error(f"❌ فشل الـ API في جلب البيانات: {data}")
            return
            
        articles = data.get("articles", [])
        
        # سجل يوضح العدد الإجمالي الذي تم استلامه من API قبل تطبيق الفلترة
        logger.info(f"📥 تم استلام {len(articles)} مقال من NewsAPI (نطاق الأمس واليوم). جاري الفلترة...")
        
        total_sent = process_and_send_articles(articles)
        
        logger.info(f"🏁 تم الانتهاء من المعالجة. الأخبار التي تجاوزت الفلتر وأُرسلت بنجاح: {total_sent}")
        
    except Exception as e:
        logger.critical(f"💥 خطأ غير متوقع أثناء تشغيل الـ Pipeline: {e}")
    finally:
        producer.close()
        logger.info("🔌 تم إغلاق اتصال Kafka بأمان.")