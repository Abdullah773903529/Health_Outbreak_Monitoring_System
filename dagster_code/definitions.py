# dagster_code/definitions.py
from dagster import Definitions, load_assets_from_modules, define_asset_job, ScheduleDefinition
# 1. قمنا بإضافة gold_assets إلى الاستيراد بجانب bronze و silver
from assets.batch import bronze, silver, gold_assets 

# 2. تحميل جميع الأصول (Assets) من الملفات الثلاثة لتغطية المستودع بالكامل
batch_assets = load_assets_from_modules([bronze, silver, gold_assets])

# 3. تعريف مهمة (Job) تجمع هذه الأصول لتشغيلها معاً
# داغستر سيرتب التنفيذ تلقائياً: Bronze ثم Silver ثم Gold بناءً على الـ dependencies
daily_batch_job = define_asset_job(
    name="daily_outbreak_batch_job",
    selection=batch_assets
)

# 4. إعداد الجدول الزمني (Schedule)
# لتشغيل خط الإنتاج بالكامل يومياً عند الساعة 2:00 صباحاً
daily_schedule = ScheduleDefinition(
    job=daily_batch_job,
    cron_schedule="0 2 * * *", 
    execution_timezone="Asia/Riyadh"  
)

# 5. تجميع كل شيء ليتعرف عليه Dagster
defs = Definitions(
    assets=batch_assets,
    jobs=[daily_batch_job],
    schedules=[daily_schedule],
)