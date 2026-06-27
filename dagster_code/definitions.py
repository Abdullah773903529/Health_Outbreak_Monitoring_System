# dagster_code/definitions.py
from dagster import Definitions, load_assets_from_modules, define_asset_job, ScheduleDefinition
from assets.batch import bronze

# 1. تحميل جميع الأصول (Assets) من ملف bronze.py
batch_assets = load_assets_from_modules([bronze])

# 2. تعريف مهمة (Job) تجمع هذه الأصول لتشغيلها معاً
daily_batch_job = define_asset_job(
    name="daily_outbreak_batch_job",
    selection=batch_assets
)

# 3. إعداد الجدول الزمني (Schedule)
# هذا الجدول سيقوم بتشغيل المهمة يومياً عند الساعة 2:00 صباحاً بتوقيت النظام
daily_schedule = ScheduleDefinition(
    job=daily_batch_job,
    cron_schedule="0 2 * * *", 
    execution_timezone="Asia/Riyadh"  # التسمية الصحيحة في داغستر هي execution_timezone
)

# 4. تجميع كل شيء ليتعرف عليه Dagster
defs = Definitions(
    assets=batch_assets,
    jobs=[daily_batch_job],
    schedules=[daily_schedule],
)