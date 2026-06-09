from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",  
    client_id='test'                      
)

topic_name = "market_analysis"
num_partitions = 1
replication_factor = 1

new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

admin_client.create_topics(new_topics=[new_topic], validate_only=False)
print(f"Топик '{topic_name}' успешно создан.")

# Удаление топика (раскомментируйте строку ниже для удаления)
# admin_client.delete_topics(topics=[topic_name])
# print(f"Топик '{topic_name}' успешно удалён.") 
