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
# 2. الكلمات المفتاحية الموسعة للفلترة
# ==========================================
INFECTIOUS_DISEASES = [
    # أمراض وبائية شديدة (Pandemic Potential)
    "ebola", "ebola virus", "marburg", "marburg virus", "lassa fever", "lassa",
    "crimean-congo", "cchf", "rift valley fever", "rift valley",
    "nipah", "nipah virus", "hendra", "hendra virus",
    "sars", "mers", "covid-19", "covid", "coronavirus", "sars-cov-2",
    
    # إنفلونزا الطيور والخنازير
    "avian influenza", "bird flu", "avian flu",
    "h5n1", "h5n6", "h5n8", "h7n9", "h7n7", "h9n2", "h10n3",
    "swine flu", "h1n1", "h3n2", "pandemic flu", "pandemic influenza",
    
    # أمراض بكتيرية خطيرة
    "cholera", "plague", "bubonic plague", "pneumonic plague",
    "anthrax", "typhoid", "typhoid fever", "typhus",
    "diphtheria", "tetanus", "botulism",
    "meningitis", "meningococcal", "leptospirosis",
    "shigellosis", "shigella", "legionnaires", "legionella",
    
    # أمراض فيروسية خطيرة
    "measles", "rubeola", "monkeypox", "mpox", "orthopoxvirus",
    "polio", "poliomyelitis", "rabies", "lyssavirus",
    "dengue", "dengue fever", "dengue hemorrhagic",
    "yellow fever", "west nile", "west nile virus",
    "zika", "zika virus", "chikungunya",
    
    # أمراض متوسطة
    "malaria", "plasmodium", "tuberculosis", "tb",
    "hiv", "aids", "influenza", "flu", "seasonal flu",
    "norovirus", "norwalk virus", "rotavirus",
    "salmonella", "salmonellosis", "listeria", "listeriosis",
    "hepatitis a", "hepatitis e", "hepatitis outbreak",
    "pertussis", "whooping cough", "scarlet fever",
    "e. coli", "escherichia coli", "cryptosporidium", "cryptosporidiosis",
    
    # أمراض ناشئة ومهملة
    "hmpv", "human metapneumovirus", "rsv", "respiratory syncytial virus",
    "adenovirus", "enterovirus", "rhinovirus",
    "mrsa", "candida auris", "c. auris",
    "oropouche", "oropouche virus", "oropouche fever",
    "leishmaniasis", "kala-azar", "dracunculiasis", "guinea worm",
    "chapare", "chapare virus", "sudan virus",
    
    # مصطلحات عامة للتفشي
    "virus", "infection", "infectious disease", "viral disease",
    "bacterial infection", "bacterial outbreak", "fungal infection",
    "parasitic disease", "zoonotic disease", "zoonotic virus",
    "vector-borne disease", "waterborne disease", "foodborne illness",
    "screwworm", "new world screwworm",
    
    # أمراض إضافية
    "chagas", "chagas disease", "trypanosomiasis", "sleeping sickness",
    "onchocerciasis", "river blindness", "schistosomiasis", "bilharzia",
    "lymphatic filariasis", "elephantiasis", "trachoma",
    "buruli ulcer", "yaws", "leprosy", "hansen's disease",
    "melioidosis", "glanders", "brucellosis", "brucella",
    "q fever", "typhus", "scrub typhus", "murine typhus",
    "toxoplasmosis", "toxoplasma", "trichinellosis", "trichinella",
    "echinococcosis", "hydatid disease", "cysticercosis",
    "mycetoma", "chromoblastomycosis", "sporotrichosis",
    "histoplasmosis", "coccidioidomycosis", "valley fever",
    "aspergillosis", "mucormycosis", "black fungus",
    "cryptococcosis", "candida infection",
    
    # مصطلحات للأمراض غير المعروفة
    "disease x", "unknown virus", "unknown pathogen",
    "mystery illness", "mysterious disease", "mysterious illness",
    "undiagnosed disease", "unidentified virus", "unidentified illness",
    "novel virus", "novel pathogen", "new virus", "new pathogen",
    "new strain", "new variant", "mutated virus", "mutated strain",
    "recombinant virus", "hybrid virus",
    "cross-species transmission", "animal-to-human transmission",
    "spillover", "spillover event"
]

OUTBREAK_CONTEXTS = [
    # مصطلحات التفشي الأساسية
    "outbreak", "epidemic", "pandemic", "endemic",
    "cluster of cases", "disease cluster",
    "surge", "surge in cases", "spike in cases",
    "rise in cases", "increase in cases", "growing number",
    "sharp increase", "dramatic increase", "alarming increase",
    "sudden increase", "rapid increase",
    
    # مصطلحات الحالات
    "first case", "first confirmed case", "index case", "patient zero",
    "confirmed case", "confirmed cases", "suspected case", "suspected cases",
    "probable case", "probable cases", "new case", "new cases",
    "additional cases", "more cases", "reported cases",
    "case count", "case tally", "death toll", "fatalities",
    "positive case", "positive cases", "tested positive",
    "case fatality", "mortality rate",
    
    # مصطلحات الكشف والتشخيص
    "detected", "identified", "discovered", "found",
    "diagnosed", "diagnosis", "laboratory confirmed",
    "tested positive for", "samples tested", "screening",
    "surveillance", "active surveillance", "passive surveillance",
    "contact tracing", "contact investigation",
    "genomic sequencing", "genome sequencing",
    
    # مصطلحات الانتشار
    "spread", "spreading", "rapid spread", "widespread",
    "transmission", "community transmission", "local transmission",
    "human-to-human", "person-to-person", "airborne transmission",
    "droplet transmission", "aerosol transmission",
    "vector-borne", "mosquito-borne", "tick-borne",
    "waterborne", "foodborne", "bloodborne",
    "contagious", "highly contagious", "infectious", "communicable",
    "superspreader", "superspreading event",
    
    # مصطلحات الاستجابة والطوارئ
    "health alert", "health warning", "health advisory",
    "public health emergency", "public health emergency of international concern",
    "pheic", "global health emergency",
    "emergency response", "emergency committee",
    "emergency use authorization", "emergency use listing",
    "lockdown", "quarantine", "isolation", "containment",
    "travel restriction", "travel advisory", "travel warning",
    "border closure", "screening measures",
    
    # مصطلحات الإبلاغ
    "WHO", "World Health Organization", "CDC", "Centers for Disease Control",
    "ECDC", "health ministry", "ministry of health",
    "reported by", "confirmed by", "announced by",
    "according to", "statement from", "press release",
    "situation report", "disease outbreak news",
    
    # مصطلحات إضافية
    "hospitalized", "hospitalization", "intensive care",
    "isolation ward", "treatment center", "field hospital",
    "vaccination campaign", "vaccination drive", "immunization",
    "containment measures", "control measures", "prevention measures",
    "public health measures", "health protocols",
    "disease surveillance", "early warning", "rapid response",
    "hotspot", "red zone", "high risk area",
    "epicenter", "ground zero",
    
    # مصطلحات للحيوانات
    "culling", "animal reservoir", "intermediate host",
    "wet market", "live animal market",
    "livestock", "poultry", "wildlife"
]

EXCLUDED_KEYWORDS = [
    # ترفيه ومشاهير
    "celebrity", "concert", "movie", "film", "music", "song", "album",
    "tv show", "television", "netflix", "hollywood", "bollywood",
    "actor", "actress", "singer", "musician", "artist",
    "award", "oscar", "grammy", "red carpet",
    
    # رياضة
    "sports", "football", "soccer", "basketball", "baseball",
    "tennis", "golf", "olympics", "world cup", "championship",
    "player", "athlete", "coach", "team", "match", "tournament",
    
    # تكنولوجيا وأعمال
    "cryptocurrency", "bitcoin", "ethereum", "blockchain", "nft",
    "stock market", "wall street", "investment", "startup",
    "software", "app", "application", "update", "upgrade",
    "computer virus", "malware", "ransomware", "hacker", "cyber attack",
    "data breach", "phishing", "spam", "ddos",
    
    # صحة غير معدية
    "cancer", "diabetes", "heart disease", "heart attack", "stroke",
    "alzheimer", "dementia", "parkinson", "arthritis",
    "obesity", "overweight", "weight loss", "diet", "nutrition",
    "mental health", "depression", "anxiety", "stress", "therapy",
    "wellness", "meditation", "yoga", "mindfulness",
    "cosmetic", "plastic surgery", "botox",
    
    # موضة وأسلوب حياة
    "fashion", "style", "beauty", "makeup", "skincare",
    "celebrity style", "outfit", "clothing", "accessories",
    "recipe", "cooking", "food", "restaurant", "cuisine",
    "travel", "vacation", "hotel", "resort", "destination",
    "home decor", "interior design", "diy",
    
    # حيوانات أليفة وعلاقات
    "pet", "dog", "cat", "puppy", "kitten", "pet care",
    "relationship", "dating", "marriage", "divorce", "wedding",
    "parenting", "pregnancy", "baby", "childbirth",
    
    # غير ذلك
    "sleep", "insomnia", "fitness", "exercise", "workout",
    "body type", "body shape", "aging", "anti-aging",
    "clinical trial", "drug trial", "pharmaceutical stock",
    "real estate", "mortgage", "insurance",
    "politics", "election", "vote", "campaign",
    "weather", "climate", "hurricane", "earthquake",
    "war", "conflict", "military", "weapon"
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
            logger.warning(f" المحاولة {retries} فشلت. إعادة المحاولة خلال {delay} ثوانٍ...")
            if retries == max_retries:
                logger.error(" تم استنفاد المحاولات للاتصال بـ API.")
                raise e
            time.sleep(delay)
            delay *= backoff_factor

def process_and_send_articles(articles: list) -> int:
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
    logger.info(" بدء تشغيل الـ News Outbreak Producer...")
    try:
        data = fetch_news_with_retry()
        if data.get("status") != "ok":
            logger.error(f" فشل الـ API في جلب البيانات: {data}")
            return
            
        articles = data.get("articles", [])
        
        # سجل يوضح العدد الإجمالي الذي تم استلامه من API قبل تطبيق الفلترة
        logger.info(f" تم استلام {len(articles)} مقال من NewsAPI (نطاق الأمس واليوم). جاري الفلترة...")
        
        total_sent = process_and_send_articles(articles)
        
        logger.info(f" تم الانتهاء من المعالجة. الأخبار التي تجاوزت الفلتر وأُرسلت بنجاح: {total_sent}")
        
    except Exception as e:
        logger.critical(f" خطأ غير متوقع أثناء تشغيل الـ Pipeline: {e}")
    finally:
        producer.close()
        logger.info("🔌 تم إغلاق اتصال Kafka بأمان.")