from src.kafka_container import KafkaContainer
import pandas as pd

kafka = KafkaContainer(bootstrap_servers='localhost:9092')
kafka.create_topic("market_analysis")

df = pd.DataFrame({'product': ['Товар1', 'Товар2', 'Товар3'],'price': [100, 200, 300],'quantity': [5, 10, 15]})

kafka.send_dataframe(df, "market_analysis")
result = kafka.read_to_dataframe("market_analysis")
print(result)

# Удаляем топик
# kafka.delete_topic("market_analysis")

kafka.close()
