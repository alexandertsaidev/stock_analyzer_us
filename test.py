from kafka import KafkaProducer

try:
    producer = KafkaProducer(bootstrap_servers=["kafka:9092"])
    print("✅ 連線成功")
    producer.close()
except Exception as e:
    print(f"❌ 連線失敗: {e}")