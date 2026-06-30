# dagster_code/definitions.py
from dagster import Definitions, load_assets_from_modules, define_asset_job, ScheduleDefinition, AssetSelection

# 1. استيراد ملفات مسار الـ Batch
from assets.batch import bronze, silver, gold_assets 

# 2. استيراد المنتج فقط (إزالة المستهلك من هنا)
from assets.streaming import news_producer

# 3. تحميل الأصول
batch_assets = load_assets_from_modules([bronze, silver, gold_assets])
streaming_assets = load_assets_from_modules([news_producer])

all_assets = batch_assets + streaming_assets

batch_job_selection = AssetSelection.assets(*batch_assets)
streaming_producer_selection = AssetSelection.keys("fetch_and_produce_outbreak_news")

monthly_batch_job = define_asset_job(name="monthly_outbreak_batch_job", selection=batch_job_selection)
monthly_schedule = ScheduleDefinition(job=monthly_batch_job, cron_schedule="0 2 1 * *", execution_timezone="Asia/Riyadh")

streaming_producer_job = define_asset_job(name="streaming_news_producer_job", selection=streaming_producer_selection)
streaming_schedule = ScheduleDefinition(job=streaming_producer_job, cron_schedule="*/3 * * * *", execution_timezone="Asia/Riyadh")

defs = Definitions(
    assets=all_assets,
    jobs=[monthly_batch_job, streaming_producer_job],
    schedules=[monthly_schedule, streaming_schedule],
)