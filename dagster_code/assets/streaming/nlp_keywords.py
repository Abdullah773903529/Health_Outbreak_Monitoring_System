"""
الكلمات المفتاحية والأنماط لاستخراج الأمراض والدول
"""

# ========================
# قائمة الأمراض للبحث المباشر (مرتبة حسب الأولوية - الأطول أولاً)
# ========================
DISEASE_KEYWORDS = [
    # أمراض مركبة (الأطول أولاً لتجنب التطابق الجزئي)
    "avian influenza", "bird flu", "h5n1", "h7n9", "h1n1",
    "lassa fever", "marburg disease", "ebola disease",
    "crimean-congo hemorrhagic fever", "crimean-congo haemorrhagic fever", "cchf",
    "rift valley fever", "nipah virus", "nipah virus disease",
    "hendra virus", "hendra virus disease",
    "yellow fever", "sylvatic yellow fever",
    "dengue hemorrhagic fever", "dengue shock syndrome", "dengue fever",
    "west nile virus", "west nile virus infection",
    "whooping cough", "whooping cough due to bordetella pertussis",
    "typhoid fever", "bubonic plague", "pneumonic plague",
    "hepatitis a", "hepatitis e", "acute hepatitis a", "acute hepatitis e",
    "scarlet fever", "guinea worm disease", "guinea worm",
    "candida auris", "c. auris",
    "e. coli infection", "escherichia coli",
    "sudan virus disease", "sudan virus",
    "human metapneumovirus", "human respiratory syncytial virus",
    "legionnaires disease", "legionnaires",
    "kala-azar", "japanese encephalitis", "japanese encephalitis virus disease",
    "viral pneumonia", "valley fever",
    
    # أمراض مفردة
    "covid-19", "covid", "coronavirus", "sars-cov-2",
    "tuberculosis", "leptospirosis", "shigellosis",
    "salmonellosis", "listeriosis", "cryptosporidiosis",
    "leishmaniasis", "dracunculiasis", "coccidioidomycosis",
    "chikungunya", "poliomyelitis", "meningococcal",
    "adenovirus", "enterovirus", "hantavirus",
    "norovirus", "rotavirus", "parainfluenza",
    "mycoplasma", "staphylococcus", "creutzfeldt-jakob",
    
    # أمراض قصيرة
    "monkeypox", "diphtheria", "pertussis", "influenza",
    "meningitis", "hepatitis", "salmonella", "listeria",
    "cryptosporidium", "legionella", "plasmodium",
    
    # أمراض قصيرة جداً
    "cholera", "dengue", "measles", "malaria", "ebola",
    "marburg", "lassa", "polio", "rabies", "anthrax",
    "tetanus", "typhoid", "typhus", "plague", "zika",
    "sars", "mers", "mpox", "hiv", "aids",
    "hmpv", "rsv", "mrsa", "tb", "flu",
    "h5n1", "h7n9", "h1n1", "cchf",
    "oropouche", "chapare", "botulism",
    "sepsis", "rubeola", "staph",
    "ebola", "marburg",
]

# ========================
# قائمة الدول والمناطق للبحث المباشر (مرتبة حسب الطول)
# ========================
COUNTRY_KEYWORDS = [
    # أسماء مركبة (الأطول أولاً)
    "democratic republic of congo", "south sudan", "sierra leone",
    "south africa", "burkina faso", "saudi arabia",
    "united states", "united kingdom", "south korea",
    "sri lanka", "north carolina", "south carolina",
    "new york", "new delhi", "addis ababa", "dar es salaam",
    "new hampshire", "new jersey", "new mexico",
    "north dakota", "rhode island", "south dakota",
    "west virginia", "costa rica",
    
    # دول أفريقيا
    "congo", "uganda", "nigeria", "ethiopia", "kenya",
    "tanzania", "rwanda", "burundi", "somalia", "ghana",
    "guinea", "liberia", "mozambique", "angola", "cameroon",
    "chad", "niger", "mali", "egypt", "madagascar",
    "zimbabwe", "zambia", "malawi", "senegal", "sudan",
    "eritrea", "djibouti", "gabon", "namibia", "botswana",
    
    # مدن أفريقية
    "kinshasa", "lagos", "nairobi", "cairo", "abuja",
    "kampala", "kigali", "mogadishu", "accra", "conakry",
    "monrovia", "freetown", "johannesburg", "khartoum",
    
    # الشرق الأوسط
    "yemen", "syria", "iraq", "iran", "afghanistan",
    "pakistan", "lebanon", "jordan", "palestine", "gaza",
    "israel", "kuwait", "qatar", "bahrain", "oman",
    "united arab emirates", "uae",
    
    # مدن شرق أوسطية
    "sanaa", "damascus", "baghdad", "tehran", "kabul",
    "riyadh", "beirut", "amman", "dubai", "doha",
    
    # آسيا
    "india", "bangladesh", "china", "indonesia",
    "philippines", "myanmar", "burma", "thailand",
    "vietnam", "nepal", "malaysia", "singapore",
    "taiwan", "mongolia", "laos", "cambodia",
    
    # مدن آسيوية
    "new delhi", "beijing", "jakarta", "manila",
    "bangkok", "hanoi", "dhaka", "kathmandu",
    "shanghai", "tokyo", "seoul", "mumbai",
    
    # أمريكا اللاتينية
    "brazil", "haiti", "venezuela", "colombia",
    "peru", "bolivia", "ecuador", "argentina", "mexico",
    "chile", "uruguay", "paraguay", "panama",
    "cuba", "dominican republic", "guatemala", "honduras",
    "el salvador", "nicaragua", "costa rica",
    
    # دول كبرى
    "usa", "uk", "britain", "england", "france",
    "germany", "italy", "spain", "canada", "australia",
    "japan", "russia", "ukraine", "turkey",
    "sweden", "norway", "denmark", "finland", "poland",
    "greece", "portugal", "ireland", "switzerland",
    "austria", "belgium", "netherlands",
    
    # ولايات أمريكية
    "california", "texas", "florida", "virginia",
    "ohio", "illinois", "washington", "georgia",
    "michigan", "pennsylvania", "arizona", "massachusetts",
    "alabama", "alaska", "colorado", "connecticut",
    "delaware", "hawaii", "idaho", "indiana",
    "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada",
    "oklahoma", "oregon", "tennessee", "utah",
    "vermont", "wisconsin", "wyoming",
]

# ========================
# أنماط spaCy للأمراض
# ========================
DISEASE_PATTERNS = [
    # أمراض مركبة
    {"label": "DISEASE", "pattern": [{"LOWER": "avian"}, {"LOWER": "influenza"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "bird"}, {"LOWER": "flu"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "lassa"}, {"LOWER": "fever"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "rift"}, {"LOWER": "valley"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "yellow"}, {"LOWER": "fever"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "west"}, {"LOWER": "nile"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "typhoid"}, {"LOWER": "fever"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "scarlet"}, {"LOWER": "fever"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "whooping"}, {"LOWER": "cough"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "guinea"}, {"LOWER": "worm"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "candida"}, {"LOWER": "auris"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "e."}, {"LOWER": "coli"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "escherichia"}, {"LOWER": "coli"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "nipah"}, {"LOWER": "virus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "hendra"}, {"LOWER": "virus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "sudan"}, {"LOWER": "virus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "human"}, {"LOWER": "metapneumovirus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "dengue"}, {"LOWER": "fever"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "dengue"}, {"LOWER": "hemorrhagic"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "hepatitis"}, {"LOWER": "a"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "hepatitis"}, {"LOWER": "e"}]},
    
    # أمراض مفردة
    {"label": "DISEASE", "pattern": [{"LOWER": "covid-19"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "covid"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "coronavirus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "sars-cov-2"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "ebola"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "marburg"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "cholera"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "dengue"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "measles"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "malaria"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "monkeypox"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "mpox"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "polio"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "poliomyelitis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "zika"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "rabies"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "anthrax"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "tetanus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "plague"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "typhus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "typhoid"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "diphtheria"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "pertussis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "meningitis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "meningococcal"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "leptospirosis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "shigellosis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "shigella"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "salmonella"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "salmonellosis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "listeria"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "listeriosis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "hantavirus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "hiv"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "aids"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "influenza"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "flu"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "tuberculosis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "tb"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "sars"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "mers"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "nipah"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "hendra"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "h5n1"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "h7n9"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "h1n1"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "cchf"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "norovirus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "rotavirus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "hmpv"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "rsv"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "adenovirus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "enterovirus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "mrsa"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "botulism"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "oropouche"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "chikungunya"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "leishmaniasis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "dracunculiasis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "cryptosporidium"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "cryptosporidiosis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "legionnaires"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "legionella"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "chapare"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "kala-azar"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "sepsis"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "staphylococcus"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "mycoplasma"}]},
    {"label": "DISEASE", "pattern": [{"LOWER": "parainfluenza"}]},
]

# ========================
# أنماط spaCy للدول
# ========================
COUNTRY_PATTERNS = [
    # أفريقيا
    {"label": "COUNTRY", "pattern": [{"LOWER": "congo"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "drc"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "uganda"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "sudan"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "south"}, {"LOWER": "sudan"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "nigeria"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "ethiopia"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "kenya"}]},
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
    {"label": "COUNTRY", "pattern": [{"LOWER": "madagascar"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "zimbabwe"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "zambia"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "malawi"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "senegal"}]},
    
    # الشرق الأوسط
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
    
    # آسيا
    {"label": "COUNTRY", "pattern": [{"LOWER": "india"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "bangladesh"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "china"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "indonesia"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "philippines"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "myanmar"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "burma"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "thailand"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "vietnam"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "nepal"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "sri"}, {"LOWER": "lanka"}]},
    
    # أمريكا اللاتينية
    {"label": "COUNTRY", "pattern": [{"LOWER": "brazil"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "haiti"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "venezuela"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "colombia"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "peru"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "bolivia"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "ecuador"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "argentina"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "mexico"}]},
    
    # دول كبرى
    {"label": "COUNTRY", "pattern": [{"LOWER": "usa"}]},
    {"label": "COUNTRY", "pattern": [{"LOWER": "united"}, {"LOWER": "states"}]},
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
]

# ========================
# قاموس توحيد أسماء الأمراض (للتطابق مع قاعدة البيانات)
# ========================
DISEASE_MAPPING = {
    # COVID-19
    "covid": "COVID-19", "covid-19": "COVID-19",
    "coronavirus": "COVID-19", "sars-cov-2": "COVID-19",
    
    # Ebola
    "ebola": "EBOLA DISEASE", "ebola virus": "EBOLA DISEASE",
    "ebola disease": "EBOLA DISEASE",
    
    # Marburg
    "marburg": "MARBURG DISEASE", "marburg virus": "MARBURG DISEASE",
    "marburg disease": "MARBURG DISEASE",
    
    # Tuberculosis
    "tb": "TUBERCULOSIS OF THE RESPIRATORY SYSTEM",
    "tuberculosis": "TUBERCULOSIS OF THE RESPIRATORY SYSTEM",
    
    # Cholera
    "cholera": "CHOLERA",
    
    # Dengue
    "dengue": "DENGUE FEVER, UNSPECIFIED",
    "dengue fever": "DENGUE FEVER, UNSPECIFIED",
    
    # Measles
    "measles": "MEASLES", "rubeola": "MEASLES",
    
    # Malaria
    "malaria": "MALARIA, UNSPECIFIED",
    
    # Mpox/Monkeypox
    "mpox": "MONKEYPOX", "monkeypox": "MONKEYPOX",
    
    # Polio
    "polio": "ACUTE POLIOMYELITIS, UNSPECIFIED",
    "poliomyelitis": "ACUTE POLIOMYELITIS, UNSPECIFIED",
    
    # Zika
    "zika": "ZIKA VIRUS DISEASE", "zika virus": "ZIKA VIRUS DISEASE",
    
    # Yellow Fever
    "yellow fever": "YELLOW FEVER, UNSPECIFIED",
    "sylvatic yellow fever": "SYLVATIC YELLOW FEVER",
    
    # West Nile
    "west nile": "WEST NILE VIRUS INFECTION",
    "west nile virus": "WEST NILE VIRUS INFECTION",
    
    # Lassa
    "lassa": "LASSA FEVER", "lassa fever": "LASSA FEVER",
    
    # CCHF
    "cchf": "CRIMEAN-CONGO HAEMORRHAGIC FEVER",
    "crimean-congo": "CRIMEAN-CONGO HAEMORRHAGIC FEVER",
    "crimean-congo hemorrhagic fever": "CRIMEAN-CONGO HAEMORRHAGIC FEVER",
    "crimean-congo haemorrhagic fever": "CRIMEAN-CONGO HAEMORRHAGIC FEVER",
    
    # Rift Valley
    "rift valley": "RIFT VALLEY FEVER",
    "rift valley fever": "RIFT VALLEY FEVER",
    
    # Nipah/Hendra
    "nipah": "NIPAH VIRUS DISEASE", "nipah virus": "NIPAH VIRUS DISEASE",
    "hendra": "HENDRA VIRUS DISEASE", "hendra virus": "HENDRA VIRUS DISEASE",
    
    # SARS/MERS
    "sars": "SEVERE ACUTE RESPIRATORY SYNDROME",
    "mers": "MIDDLE EAST RESPIRATORY SYNDROME",
    
    # Plague
    "plague": "PLAGUE, UNSPECIFIED",
    "bubonic plague": "BUBONIC PLAGUE",
    
    # Anthrax
    "anthrax": "ANTHRAX, UNSPECIFIED",
    "cutaneous anthrax": "CUTANEOUS ANTHRAX",
    
    # Rabies
    "rabies": "RABIES, UNSPECIFIED",
    
    # Diphtheria
    "diphtheria": "DIPHTHERIA, UNSPECIFIED",
    
    # Pertussis
    "pertussis": "WHOOPING COUGH DUE TO BORDETELLA PERTUSSIS",
    "whooping cough": "WHOOPING COUGH DUE TO BORDETELLA PERTUSSIS",
    
    # Typhoid
    "typhoid": "TYPHOID FEVER", "typhoid fever": "TYPHOID FEVER",
    
    # Typhus
    "typhus": "EPIDEMIC LOUSE-BORNE TYPHUS FEVER DUE TO RICKETTSIA PROWAZEKII",
    
    # Meningitis
    "meningitis": "BACTERIAL MENINGITIS, UNSPECIFIED",
    "meningococcal": "MENINGOCOCCAL MENINGITIS",
    
    # Hepatitis
    "hepatitis a": "ACUTE HEPATITIS A", "hepatitis e": "ACUTE HEPATITIS E",
    
    # HIV/AIDS
    "hiv": "HUMAN IMMUNODEFICIENCY VIRUS DISEASE WITHOUT MENTION OF ASSOCIATED DISEASE OR CONDITION, CLINICAL STAGE UNSPECIFIED",
    "aids": "HUMAN IMMUNODEFICIENCY VIRUS DISEASE WITHOUT MENTION OF ASSOCIATED DISEASE OR CONDITION, CLINICAL STAGE UNSPECIFIED",
    "hiv/aids": "HUMAN IMMUNODEFICIENCY VIRUS DISEASE WITHOUT MENTION OF ASSOCIATED DISEASE OR CONDITION, CLINICAL STAGE UNSPECIFIED",
    
    # Influenza
    "flu": "INFLUENZA WITH OTHER MANIFESTATIONS, SEASONAL INFLUENZA VIRUS IDENTIFIED",
    "influenza": "INFLUENZA WITH OTHER MANIFESTATIONS, SEASONAL INFLUENZA VIRUS IDENTIFIED",
    
    # Avian/Bird Flu
    "bird flu": "INFLUENZA DUE TO IDENTIFIED ZOONOTIC OR PANDEMIC INFLUENZA VIRUS",
    "avian influenza": "INFLUENZA DUE TO IDENTIFIED ZOONOTIC OR PANDEMIC INFLUENZA VIRUS",
    "h5n1": "INFLUENZA DUE TO IDENTIFIED ZOONOTIC OR PANDEMIC INFLUENZA VIRUS",
    "h7n9": "INFLUENZA DUE TO IDENTIFIED ZOONOTIC OR PANDEMIC INFLUENZA VIRUS",
    "h1n1": "INFLUENZA DUE TO IDENTIFIED ZOONOTIC OR PANDEMIC INFLUENZA VIRUS",
    
    # Hantavirus
    "hantavirus": "HANTAVIRUS PULMONARY SYNDROME",
    
    # Leptospirosis
    "leptospirosis": "LEPTOSPIROSIS, UNSPECIFIED",
    
    # Shigellosis
    "shigellosis": "SHIGELLOSIS DUE TO SHIGELLA DYSENTERIAE",
    "shigella": "SHIGELLOSIS DUE TO SHIGELLA DYSENTERIAE",
    
    # Salmonellosis
    "salmonella": "INFECTIONS DUE TO OTHER SALMONELLA",
    "salmonellosis": "INFECTIONS DUE TO OTHER SALMONELLA",
    
    # Listeriosis
    "listeria": "LISTERIOSIS, UNSPECIFIED",
    "listeriosis": "LISTERIOSIS, UNSPECIFIED",
    
    # Botulism
    "botulism": "BOTULISM",
    
    # Tetanus
    "tetanus": "TETANUS, UNSPECIFIED",
    
    # Legionnaires
    "legionnaires": "LEGIONNAIRES DISEASE",
    "legionnaires disease": "LEGIONNAIRES DISEASE",
    
    # RSV
    "rsv": "HUMAN RESPIRATORY SYNCYTIAL VIRUS",
    
    # HMPV
    "hmpv": "PNEUMONIA DUE TO HUMAN METAPNEUMOVIRUS",
    "human metapneumovirus": "PNEUMONIA DUE TO HUMAN METAPNEUMOVIRUS",
    
    # Adenovirus
    "adenovirus": "PNEUMONIA DUE TO ADENOVIRUS",
    
    # Enterovirus
    "enterovirus": "ENTEROVIRAL VESICULAR STOMATITIS",
    
    # Chikungunya
    "chikungunya": "CHIKUNGUNYA MOSQUITO-BORNE VIRAL FEVER",
    
    # Oropouche
    "oropouche": "OROPOUCHE VIRUS DISEASE",
    
    # Leishmaniasis
    "leishmaniasis": "LEISHMANIASIS, UNSPECIFIED",
    "kala-azar": "LEISHMANIASIS, UNSPECIFIED",
    
    # Scarlet Fever
    "scarlet fever": "SCARLET FEVER",
    
    # Dracunculiasis
    "dracunculiasis": "DRACUNCULIASIS",
    "guinea worm": "DRACUNCULIASIS",
    
    # Sudan Virus
    "sudan virus": "SUDAN VIRUS DISEASE",
    "chapare": "SUDAN VIRUS DISEASE",
    
    # Japanese Encephalitis
    "japanese encephalitis": "JAPANESE ENCEPHALITIS VIRUS DISEASE",
    
    # Sepsis
    "sepsis": "SEPSIS WITHOUT SEPTIC SHOCK",
    
    # Viral Pneumonia
    "viral pneumonia": "VIRAL PNEUMONIA",
}

# ========================
# قاموس توحيد أسماء الدول
# ========================
COUNTRY_MAPPING = {
    "usa": "UNITED STATES", "united states": "UNITED STATES",
    "uk": "UNITED KINGDOM", "united kingdom": "UNITED KINGDOM",
    "britain": "UNITED KINGDOM", "england": "UNITED KINGDOM",
    "drc": "DEMOCRATIC REPUBLIC OF CONGO",
    "kinshasa": "DEMOCRATIC REPUBLIC OF CONGO",
    "democratic republic of congo": "DEMOCRATIC REPUBLIC OF CONGO",
    "congo": "CONGO",
    "burma": "MYANMAR", "myanmar": "MYANMAR",
    "south korea": "SOUTH KOREA",
}

# الولايات الأمريكية
US_STATES = [
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