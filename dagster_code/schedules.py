from dagster import schedule

@schedule(
    cron_schedule="*/5 * * * *",  # كل 5 دقائق
    job_name="fraud_detection_pipeline",  # اسم الجوب الموجود في definitions.py
    execution_timezone="UTC"
)
def fraud_detection_schedule():
    """
    جدولة لتشغيل نظام كشف الاحتيال كل 5 دقائق
    """
    # يجب أن ترجع dict فارغ أو مع asset_selection
    return {}