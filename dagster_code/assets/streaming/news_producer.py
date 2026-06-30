import os
import json
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests
from kafka import KafkaProducer

load_dotenv()

# ==========================================================
# 1. الإعدادات العامة (Configuration)
# ==========================================================
API_KEY = os.getenv("NEWS_API_KEY", "YOUR_NEWS_API_KEY")
NEWS_API_URL = "https://newsapi.org/v2/everything"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "outbreak_alerts")
REQUEST_TIMEOUT = 20

# ==========================================================
# 2. الكلمات المفتاحية والتصفية الذكية
# ==========================================================
# عوامل مسببة للأمراض المعدية المستهدفة بالمراقبة
INFECTIOUS_DISEASES = [
    "cholera", "ebola", "covid", "covid-19", "influenza", "flu", "bird flu", 
    "avian influenza", "west nile", "h5n1", "measles", "mpox", "monkeypox", 
    "malaria", "dengue", "polio", "zika", "tuberculosis", "virus", "infection", 
    "bacterial outbreak", "screwworm"
]

# سياق يدل على تفشي نشط وحالات حية (وليس مقالات عامة)
OUTBREAK_CONTEXTS = [
    "outbreak", "epidemic", "pandemic", "first case", "confirmed case", 
    "confirmed cases", "new cases", "surge", "cluster", "detected", 
    "health alert", "emergency", "spread", "transmission", "infected"
]

# الكلمات المستبعدة تماماً (تصفية الضوضاء العامة وفيروسات الحاسوب)
EXCLUDED_KEYWORDS = [
    "celebrity", "concert", "movie", "music", "sleep", "diet", "fitness", 
    "relationship", "pet", "recipe", "weight loss", "body type", "aging", 
    "wellness", "cancer", "diabetes", "heart disease", "cyber", "computer virus", 
    "software", "hacker", "malware", "mental health", "clinical trial"
]

# ==========================================================
# 3. إعداد السجلات والـ Kafka Producer
# ==========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    acks="all"
)

# ذاكرة مؤقتة لمنع التكرار أثناء التشغيل الحالية
seen_urls = set()
seen_titles = set()


# ==========================================================
# 4. دالات المساعدة والمعالجة (Core Functions)
# ==========================================================
def is_relevant_outbreak(article: dict) -> bool:
    """
    التحقق الذكي من صلة الخبر بالتفشي الصحي الحقيقي.
    يجب أن يحتوي النص على مسبب مرض معدي + سياق تفشي حقيقي، وخلوه من المستبعدات.
    """
    title = (article.get("title") or "").lower()
    desc = (article.get("description") or "").lower()
    full_text = f"{title} {desc}"

    # 1. الفحص الأول: استبعاد الضوضاء
    if any(keyword in full_text for keyword in EXCLUDED_KEYWORDS):
        return False

    # 2. الفحص الثاني: التأكد من وجود مرض معدي مستهدف
    has_disease = any(disease in full_text for disease in INFECTIOUS_DISEASES)
    
    # 3. الفحص الثالث: التأكد من وجود سياق يدل على تفشي نشط لحالات
    has_outbreak_context = any(context in full_text for context in OUTBREAK_CONTEXTS)

    # يجب أن تتحقق الشروط معاً لضمان جودة البيانات وفائدتها للمشروع
    return has_disease and has_outbreak_context


def fetch_news_with_retry(max_retries=3, backoff_factor=2) -> dict:
    """
    جلب البيانات من الـ API مع دعم إعادة المحاولة التلقائية والتراجع الأسّي في حال حدوث خطأ شبكة.
    """
    # تحسين الاستعلام المباشر في الـ API لفلترة النتائج من المصدر
    params = {
        "q": '("outbreak" OR "epidemic" OR "pandemic" OR "new cases") AND ("virus" OR "infection" OR "disease" OR "cholera" OR "ebola" OR "dengue")',
        "language": "en",
        "sortBy": "publishedAt",
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
            logger.warning(f"⚠️ المحاولة {retries} فشلت بسبب: {e}. إعادة المحاولة خلال {delay} ثوانٍ...")
            if retries == max_retries:
                logger.error("❌ تم استنفاد جميع محاولات إعادة الاتصال بالـ API.")
                raise e
            time.sleep(delay)
            delay *= backoff_factor


def process_and_send_articles(articles: list) -> int:
    """معالجة قائمة الأخبار وتصفيتها وإرسالها إلى Kafka."""
    sent_count = 0
    
    for article in articles:
        title = (article.get("title") or "").strip()
        url = article.get("url")

        if not title or not url:
            continue

        # التحقق من جودة ومناسبة محتوى الخبر لمشروع الأوبئة
        if not is_relevant_outbreak(article):
            continue

        # منع تكرار نفس الخبر
        title_lower = title.lower()
        if url in seen_urls or title_lower in seen_titles:
            continue

        seen_urls.add(url)
        seen_titles.add(title_lower)

        # تجهيز الرسالة بهيكلية نظيفة
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

        # الإرسال إلى Kafka topic
        producer.send(KAFKA_TOPIC, key=title.encode("utf-8"), value=message)
        sent_count += 1
        logger.info(f"✅ تم إرسال تنبيه تفشي مؤكد: {title}")
        
    producer.flush()
    return sent_count


# ==========================================================
# 5. دالة التشغيل الرئيسية (Main)
# ==========================================================
def main():
    logger.info("🚀 بدء تشغيل الـ News Outbreak Producer المطور...")
    try:
        data = fetch_news_with_retry()
        
        if data.get("status") != "ok":
            logger.error(f"فشل الـ API في إعادة بيانات صحيحة: {data}")
            return

        articles = data.get("articles", [])
        logger.info(f"تم جلب {len(articles)} خبر خام من الـ API. جاري التصفية الفائقة...")
        
        total_sent = process_and_send_articles(articles)
        logger.info(f"🏁 تم الانتهاء بنجاح. إجمالي الأخبار المصفاة المرسلة: {total_sent}")

    except Exception as e:
        logger.critical(f"💥 خطأ غير متوقع أدى إلى توقف الـ Producer: {e}")
    finally:
        producer.close()
        logger.info("🔌 تم إغلاق اتصال Kafka Producer بأمان.")


if __name__ == "__main__":
    main()