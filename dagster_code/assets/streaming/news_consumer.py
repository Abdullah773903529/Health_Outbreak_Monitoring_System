import os
import logging
import uuid
import re
from datetime import datetime

import spacy

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField
from clickhouse_driver import Client

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
        """إضافة أنماط الأمراض الخطيرة والدول التي تحدث فيها التفشيات"""
        ruler = self.nlp.add_pipe("entity_ruler", before="ner")
        
        patterns = [
            # ========================
            # 🔴 الفئة A - شديدة الخطورة (Pandemic Potential)
            # ========================
            {"label": "DISEASE", "pattern": [{"LOWER": "ebola"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "marburg"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "lassa"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "lassa"}, {"LOWER": "fever"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "crimean-congo"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "crimean"}, {"LOWER": "congo"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "cchf"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "rift"}, {"LOWER": "valley"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "rift"}, {"LOWER": "valley"}, {"LOWER": "fever"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "nipah"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "hendra"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "sars"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "mers"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "covid-19"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "covid"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "coronavirus"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "sars-cov-2"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "h5n1"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "h7n9"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "avian"}, {"LOWER": "influenza"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "bird"}, {"LOWER": "flu"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "pandemic"}, {"LOWER": "flu"}]},
            
            # ========================
            # 🟠 الفئة B - خطيرة وسريعة الانتشار
            # ========================
            {"label": "DISEASE", "pattern": [{"LOWER": "cholera"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "yellow"}, {"LOWER": "fever"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "dengue"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "dengue"}, {"LOWER": "hemorrhagic"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "dengue"}, {"LOWER": "shock"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "measles"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "monkeypox"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "mpox"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "polio"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "poliomyelitis"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "meningitis"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "meningococcal"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "diphtheria"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "pertussis"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "whooping"}, {"LOWER": "cough"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "typhoid"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "typhoid"}, {"LOWER": "fever"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "typhus"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "plague"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "bubonic"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "bubonic"}, {"LOWER": "plague"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "pneumonic"}, {"LOWER": "plague"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "anthrax"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "rabies"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "botulism"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "tetanus"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "hepatitis"}, {"LOWER": "a"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "hepatitis"}, {"LOWER": "e"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "leptospirosis"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "legionnaires"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "shigellosis"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "shigella"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "hantavirus"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "hanta"}]},
            
            # ========================
            # 🟡 الفئة C - متوسطة الخطورة
            # ========================
            {"label": "DISEASE", "pattern": [{"LOWER": "malaria"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "zika"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "chikungunya"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "west"}, {"LOWER": "nile"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "tuberculosis"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "tb"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "hiv"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "aids"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "influenza"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "flu"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "norovirus"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "rotavirus"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "salmonella"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "salmonellosis"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "e."}, {"LOWER": "coli"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "escherichia"}, {"LOWER": "coli"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "listeria"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "listeriosis"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "scarlet"}, {"LOWER": "fever"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "oropouche"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "leishmaniasis"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "kala-azar"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "dracunculiasis"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "guinea"}, {"LOWER": "worm"}]},
            
            # ========================
            # 🟢 أمراض ناشئة (Emerging)
            # ========================
            {"label": "DISEASE", "pattern": [{"LOWER": "chapare"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "sudan"}, {"LOWER": "virus"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "hmpv"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "human"}, {"LOWER": "metapneumovirus"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "rsv"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "adenovirus"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "enterovirus"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "candida"}, {"LOWER": "auris"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "mrsa"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "cryptosporidium"}]},
            {"label": "DISEASE", "pattern": [{"LOWER": "cryptosporidiosis"}]},
            
            # ========================
            # 🌍 أفريقيا - الأكثر تفشياً
            # ========================
            {"label": "COUNTRY", "pattern": [{"LOWER": "congo"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "drc"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "kinshasa"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "uganda"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "kampala"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "sudan"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "south"}, {"LOWER": "sudan"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "nigeria"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "lagos"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "ethiopia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "addis"}, {"LOWER": "ababa"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "kenya"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "nairobi"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "tanzania"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "rwanda"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "burundi"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "somalia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "ghana"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "guinea"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "liberia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "sierra"}, {"LOWER": "leone"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "south"}, {"LOWER": "africa"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "mozambique"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "angola"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "cameroon"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "chad"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "niger"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "mali"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "burkina"}, {"LOWER": "faso"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "egypt"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "cairo"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "madagascar"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "zimbabwe"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "zambia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "malawi"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "senegal"}]},
            
            # ========================
            # 🌍 الشرق الأوسط
            # ========================
            {"label": "COUNTRY", "pattern": [{"LOWER": "yemen"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "syria"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "iraq"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "iran"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "afghanistan"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "pakistan"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "saudi"}, {"LOWER": "arabia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "lebanon"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "jordan"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "palestine"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "gaza"}]},
            
            # ========================
            # 🌍 آسيا
            # ========================
            {"label": "COUNTRY", "pattern": [{"LOWER": "india"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "bangladesh"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "china"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "beijing"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "indonesia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "philippines"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "myanmar"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "burma"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "thailand"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "vietnam"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "nepal"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "sri"}, {"LOWER": "lanka"}]},
            
            # ========================
            # 🌍 أمريكا اللاتينية
            # ========================
            {"label": "COUNTRY", "pattern": [{"LOWER": "brazil"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "haiti"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "venezuela"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "colombia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "peru"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "bolivia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "ecuador"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "argentina"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "mexico"}]},
            
            # ========================
            # 🌍 الدول الكبرى
            # ========================
            {"label": "COUNTRY", "pattern": [{"LOWER": "usa"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "united"}, {"LOWER": "states"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "america"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "uk"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "united"}, {"LOWER": "kingdom"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "britain"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "england"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "france"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "germany"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "italy"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "spain"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "canada"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "australia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "japan"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "south"}, {"LOWER": "korea"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "russia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "ukraine"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "turkey"}]},
            
            # ========================
            # 🌍 الولايات الأمريكية
            # ========================
            {"label": "COUNTRY", "pattern": [{"LOWER": "california"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "texas"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "florida"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "new"}, {"LOWER": "york"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "virginia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "south"}, {"LOWER": "carolina"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "ohio"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "illinois"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "washington"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "georgia"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "north"}, {"LOWER": "carolina"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "michigan"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "pennsylvania"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "arizona"}]},
            {"label": "COUNTRY", "pattern": [{"LOWER": "massachusetts"}]},
        ]
        
        ruler.add_patterns(patterns)
    
    def extract_disease(self, text: str) -> str:
        if not text:
            return None
        doc = self.nlp(text[:500])
        diseases = [ent.text for ent in doc.ents if ent.label_ == "DISEASE"]
        if diseases:
            return self._normalize_disease(diseases[0])
        return None
    
    def extract_country(self, text: str) -> str:
        if not text:
            return None
        doc = self.nlp(text[:500])
        countries = [ent.text for ent in doc.ents if ent.label_ == "COUNTRY"]
        if countries:
            return self._normalize_country(countries[0])
        gpes = [ent.text for ent in doc.ents if ent.label_ == "GPE"]
        if gpes:
            for gpe in gpes:
                normalized = self._normalize_country(gpe)
                if normalized:
                    return normalized
        return None
    
    def _normalize_disease(self, disease_name: str) -> str:
        disease_name = disease_name.strip().upper()
        mapping = {
            "COVID": "COVID-19", "CORONAVIRUS": "COVID-19", "SARS-COV-2": "COVID-19",
            "FLU": "INFLUENZA", "INFLUENZA": "INFLUENZA",
            "BIRD FLU": "AVIAN INFLUENZA", "AVIAN INFLUENZA": "AVIAN INFLUENZA",
            "H5N1": "AVIAN INFLUENZA", "H7N9": "AVIAN INFLUENZA",
            "MPOX": "MONKEYPOX", "MONKEYPOX": "MONKEYPOX",
            "TB": "TUBERCULOSIS", "TUBERCULOSIS": "TUBERCULOSIS",
            "WHOOPING COUGH": "PERTUSSIS", "PERTUSSIS": "PERTUSSIS",
            "HANTA": "HANTAVIRUS", "HANTAVIRUS": "HANTAVIRUS",
            "POLIO": "POLIO", "POLIOMYELITIS": "POLIO",
            "CCHF": "CRIMEAN-CONGO FEVER", "CRIMEAN-CONGO": "CRIMEAN-CONGO FEVER",
            "RIFT VALLEY": "RIFT VALLEY FEVER", "RIFT VALLEY FEVER": "RIFT VALLEY FEVER",
            "YELLOW FEVER": "YELLOW FEVER", "WEST NILE": "WEST NILE VIRUS",
            "DENGUE HEMORRHAGIC": "DENGUE FEVER", "DENGUE SHOCK": "DENGUE FEVER",
            "LASSA FEVER": "LASSA FEVER", "LASSA": "LASSA FEVER",
            "MARBURG": "MARBURG DISEASE", "EBOLA": "EBOLA DISEASE",
            "NIPAH": "NIPAH VIRUS", "HENDRA": "HENDRA VIRUS",
            "SARS": "SARS", "MERS": "MERS",
            "CHOLERA": "CHOLERA", "MEASLES": "MEASLES",
            "PLAGUE": "PLAGUE", "BUBONIC": "PLAGUE", "PNEUMONIC PLAGUE": "PLAGUE",
            "ANTHRAX": "ANTHRAX", "RABIES": "RABIES",
            "DIPHTHERIA": "DIPHTHERIA", "TYPHOID": "TYPHOID FEVER",
            "TYPHUS": "TYPHUS", "BOTULISM": "BOTULISM", "TETANUS": "TETANUS",
            "LEPTOSPIROSIS": "LEPTOSPIROSIS", "LEGIONNAIRES": "LEGIONNAIRES DISEASE",
            "SHIGELLA": "SHIGELLOSIS", "SHIGELLOSIS": "SHIGELLOSIS",
            "MENINGITIS": "MENINGITIS", "MENINGOCOCCAL": "MENINGITIS",
            "HEPATITIS A": "HEPATITIS A", "HEPATITIS E": "HEPATITIS E",
            "ZIKA": "ZIKA VIRUS", "CHIKUNGUNYA": "CHIKUNGUNYA",
            "MALARIA": "MALARIA", "HIV": "HIV/AIDS", "AIDS": "HIV/AIDS",
            "NOROVIRUS": "NOROVIRUS", "ROTAVIRUS": "ROTAVIRUS",
            "SALMONELLA": "SALMONELLOSIS", "SALMONELLOSIS": "SALMONELLOSIS",
            "E. COLI": "E. COLI", "ESCHERICHIA COLI": "E. COLI",
            "LISTERIA": "LISTERIOSIS", "LISTERIOSIS": "LISTERIOSIS",
            "SCARLET FEVER": "SCARLET FEVER", "OROPOUCHE": "OROPOUCHE VIRUS",
            "LEISHMANIASIS": "LEISHMANIASIS", "KALA-AZAR": "LEISHMANIASIS",
            "GUINEA WORM": "DRACUNCULIASIS", "DRACUNCULIASIS": "DRACUNCULIASIS",
            "CHAPARE": "CHAPARE VIRUS", "SUDAN VIRUS": "SUDAN VIRUS DISEASE",
            "HMPV": "HMPV", "RSV": "RSV", "ADENOVIRUS": "ADENOVIRUS",
            "CANDIDA AURIS": "CANDIDA AURIS", "MRSA": "MRSA",
            "CRYPTOSPORIDIUM": "CRYPTOSPORIDIOSIS", "CRYPTOSPORIDIOSIS": "CRYPTOSPORIDIOSIS",
        }
        return mapping.get(disease_name, disease_name)
    
    def _normalize_country(self, country_name: str) -> str:
        country_name = country_name.strip().upper()
        mapping = {
            "USA": "UNITED STATES", "U.S.": "UNITED STATES", "U.S.A.": "UNITED STATES",
            "AMERICA": "UNITED STATES",
            "UK": "UNITED KINGDOM", "U.K.": "UNITED KINGDOM",
            "BRITAIN": "UNITED KINGDOM", "ENGLAND": "UNITED KINGDOM",
            "DRC": "DR CONGO", "DR CONGO": "DR CONGO", "KINSHASA": "DR CONGO",
            "BURMA": "MYANMAR",
            "KOREA": "SOUTH KOREA", "SOUTH KOREA": "SOUTH KOREA",
        }
        us_states = [
            "ALABAMA", "ALASKA", "ARIZONA", "ARKANSAS", "CALIFORNIA", "COLORADO",
            "CONNECTICUT", "DELAWARE", "FLORIDA", "GEORGIA", "HAWAII", "IDAHO",
            "ILLINOIS", "INDIANA", "IOWA", "KANSAS", "KENTUCKY", "LOUISIANA",
            "MAINE", "MARYLAND", "MASSACHUSETTS", "MICHIGAN", "MINNESOTA",
            "MISSISSIPPI", "MISSOURI", "MONTANA", "NEBRASKA", "NEVADA",
            "NEW HAMPSHIRE", "NEW JERSEY", "NEW MEXICO", "NEW YORK",
            "NORTH CAROLINA", "NORTH DAKOTA", "OHIO", "OKLAHOMA", "OREGON",
            "PENNSYLVANIA", "RHODE ISLAND", "SOUTH CAROLINA", "SOUTH DAKOTA",
            "TENNESSEE", "TEXAS", "UTAH", "VERMONT", "VIRGINIA", "WASHINGTON",
            "WEST VIRGINIA", "WISCONSIN", "WYOMING"
        ]
        if country_name in us_states:
            return "UNITED STATES"
        return mapping.get(country_name, country_name)

# ========================
extractor = DiseaseCountryExtractor()

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
    skipped = {"no_disease": 0, "no_country": 0, "no_key": 0}
    
    for r in rows:
        text = f"{r.title or ''} {r.description or ''}"
        disease_name = extractor.extract_disease(text)
        country_name = extractor.extract_country(text)
        
        if not disease_name:
            skipped["no_disease"] += 1
            continue
        if not country_name:
            skipped["no_country"] += 1
            continue
        
        disease_key = disease_cache.get(normalize(disease_name))
        location_key = location_cache.get(normalize(country_name))
        
        if not disease_key or not location_key:
            skipped["no_key"] += 1
            continue
        
        data.append((
            uuid.UUID(str(uuid.uuid4())), disease_key, location_key,
            parse_date(r.published_at), datetime.utcnow(),
            r.title, r.source, r.url
        ))
    
    if data:
        try:
            client.execute("""
                INSERT INTO fact_outbreak_alerts
                (alert_id, disease_key, location_key, published_at, ingestion_time, title, source, url)
                VALUES
            """, data)
            logger.info(f"✅ Batch {batch_id}: inserted {len(data)} rows (no_dis: {skipped['no_disease']}, no_ctry: {skipped['no_country']}, no_key: {skipped['no_key']})")
        except Exception as e:
            logger.error(f"❌ Error: {e}")
    else:
        logger.warning(f"⚠️ Batch {batch_id}: no valid rows")

def main():
    logger.info("🚀 Starting Spark Outbreak Consumer (OUTBREAK DISEASES ONLY)")
    
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
        .option("checkpointLocation", "/opt/spark/checkpoints/outbreak_nlp_v2")
        .trigger(processingTime="10 seconds")
        .start())
    
    query.awaitTermination()

if __name__ == "__main__":
    main()