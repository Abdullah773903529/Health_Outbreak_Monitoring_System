from dagster import asset, Output, MetadataValue
import datetime
import json
import random
import time

# ============================================
# PRODUCER - توليد المعاملات المصرفية وإرسالها إلى Kafka
# ============================================
@asset
def generate_transactions():
    """
    توليد معاملات مصرفية وهمية وإرسالها إلى Kafka
    محاكاة المعاملات المصرفية بشكل مستمر
    """
    print("🏦 بدء توليد المعاملات المصرفية...")
    
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    
    # إعدادات Kafka
    bootstrap_servers = "kafka:29092"
    topic = "transactions"
    
    print(f"📤 Kafka Brokers: {bootstrap_servers}")
    print(f"📤 Sending to Topic: {topic}")
    
    # إنشاء Topic إذا لم يكن موجوداً
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topic_list = [NewTopic(name=topic, num_partitions=3, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"✅ تم إنشاء Topic: {topic}")
    except TopicAlreadyExistsError:
        print(f"ℹ️ Topic موجود مسبقاً: {topic}")
    except Exception as e:
        print(f"⚠️ تحذير: {e}")
    
    # إنشاء Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=5,
            linger_ms=10
        )
        print("✅ تم الاتصال بـ Kafka")
    except Exception as e:
        print(f"❌ فشل الاتصال بـ Kafka: {e}")
        raise
    
    # توليد المعاملات
    num_transactions = 100
    fraud_count = 0
    transactions_sent = []
    
    for i in range(num_transactions):
        is_fraud = (i % 100 == 0)  # 1% من المعاملات احتيال
        
        if not is_fraud:
            # معاملة عادية
            transaction = {
                "transaction_id": random.randint(100000, 999999),
                "user_id": random.randint(1, 100),
                "amount": round(random.uniform(5, 500), 2),
                "currency": random.choice(["USD", "EUR", "GBP"]),
                "timestamp": datetime.datetime.now().isoformat(),
                "country": random.choice(["US", "GB", "FR", "DE", "AE", "SA"]),
                "merchant": random.choice(["Amazon", "Apple", "Google", "Nike", "Shell"]),
                "device_id": f"DEV_{random.randint(1000, 9999)}",
                "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
                "is_fraud": 0
            }
        else:
            # معاملة احتيال
            fraud_count += 1
            if random.random() < 0.6:
                transaction = {
                    "transaction_id": random.randint(100000, 999999),
                    "user_id": random.randint(1, 100),
                    "amount": round(random.uniform(5000, 20000), 2),
                    "currency": "USD",
                    "timestamp": datetime.datetime.now().isoformat(),
                    "country": random.choice(["CN", "RU", "KP", "IR"]),
                    "merchant": "Unknown Merchant",
                    "device_id": f"DEV_{random.randint(1000, 9999)}",
                    "ip_address": f"10.0.{random.randint(1,255)}.{random.randint(1,255)}",
                    "is_fraud": 1
                }
            else:
                transaction = {
                    "transaction_id": random.randint(100000, 999999),
                    "user_id": random.randint(1, 100),
                    "amount": round(random.uniform(10, 99), 2),
                    "currency": "USD",
                    "timestamp": datetime.datetime.now().isoformat(),
                    "country": "US",
                    "merchant": "Small Store",
                    "device_id": f"DEV_{random.randint(1000, 9999)}",
                    "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
                    "is_fraud": 1
                }
        
        try:
            producer.send(topic, value=transaction)
            transactions_sent.append(transaction)
        except Exception as e:
            print(f"❌ فشل إرسال المعاملة {transaction['transaction_id']}: {e}")
    
    producer.flush()
    producer.close()
    
    print(f"✅ تم إرسال {num_transactions} معاملة")
    print(f"🚨 عدد معاملات الاحتيال: {fraud_count}")
    
    return Output(
        value={
            "total_transactions": num_transactions,
            "fraud_count": fraud_count,
            "topic": topic,
            "sample": transactions_sent[:5]  # عينة من 5 معاملات
        },
        metadata={
            "total_transactions": num_transactions,
            "fraud_count": fraud_count,
            "fraud_percentage": round((fraud_count / num_transactions) * 100, 2),
            "topic": topic,
            "status": "✅ تم إرسال المعاملات بنجاح"
        }
    )