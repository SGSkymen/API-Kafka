import pandas as pd
import json
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'market_analysis'
CSV_FILE = 'market_analysis.csv'
CHUNK_SIZE = 100

producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
counter = 0

for chunk in pd.read_csv(CSV_FILE, delimiter=';', chunksize=CHUNK_SIZE):
    dict_to_kafka = chunk.to_dict()
    data = json.dumps(dict_to_kafka, default=str).encode('utf-8')

    key = str(counter).encode()

    producer.send(topic=TOPIC_NAME, key=key, value=data)

    print(f"Чанк {counter} отправлен в топик '{TOPIC_NAME}'.")

    counter += 1

print("Все данные успешно отправлены в Kafka.")
