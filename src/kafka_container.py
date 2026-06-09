from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import json

class KafkaContainer:
    def __init__(self, bootstrap_servers='localhost:9092', client_id='test'):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id
        )
        self.producer = KafkaProducer(bootstrap_servers=[bootstrap_servers])
    
    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        self.admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Топик '{topic_name}' успешно создан.")

    def delete_topic(self, topic_name):
        self.admin_client.delete_topics(topics=[topic_name])
        print(f"Топик '{topic_name}' успешно удалён.")
    
    def send_dataframe(self, dataframe, topic_name, chunk_size=100):
        counter = 0
        for start in range(0, len(dataframe), chunk_size):
            chunk = dataframe.iloc[start:start+chunk_size]
            data = json.dumps(chunk.to_dict(), default=str).encode('utf-8')
            key = str(counter).encode()
            self.producer.send(topic=topic_name, key=key, value=data)
            print(f"Чанк {counter} отправлен в топик '{topic_name}'.")
            counter += 1
        print("Все данные успешно отправлены в Kafka.")
    
    def read_to_dataframe(self, topic_name, max_messages=10):
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=[self.bootstrap_servers],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='market_analysis_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        
        records = []
        count = 0
        for message in consumer:
            records.append(message.value)
            count += 1
            if count >= max_messages:
                break
        
        consumer.close()
        return pd.DataFrame(records)
    
    def close(self):
        self.admin_client.close()
        self.producer.close()
