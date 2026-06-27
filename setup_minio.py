from minio import Minio
from minio.error import S3Error
import os

# إعدادات الاتصال بـ MinIO
client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "outbreak-data"

def setup_storage():
    # 1. إنشاء البكت إذا لم يكن موجوداً
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' created.")
    else:
        print(f"ℹ️ Bucket '{bucket_name}' already exists.")

    # 2. رفع ملفات الـ CSV الموجودة في مجلد data
    local_data_path = "./data" 
    for filename in os.listdir(local_data_path):
        if filename.endswith(".csv"):
            client.fput_object(
                bucket_name, filename, os.path.join(local_data_path, filename)
            )
            print(f"⬆️ File '{filename}' uploaded successfully.")

if __name__ == "__main__":
    setup_storage()