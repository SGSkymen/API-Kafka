from kafka import KafkaConsumer
import json

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'market_analysis'
AUTO_OFFSET_RESET = 'earliest'  # Чтение сообщений с начала, если они ещё не были обработаны


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset=AUTO_OFFSET_RESET,
    enable_auto_commit=True,
    group_id='market_analysis_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Ожидание сообщений из топика '{TOPIC_NAME}'...")

for message in consumer:
    key = message.key.decode('utf-8') if message.key else 'None'
    value = message.value
    print(f"Получено сообщение - Ключ: {key}, Данные: {value}")

print("Чтение завершено.")
