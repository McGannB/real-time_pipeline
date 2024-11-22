from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

#Initialize Elasticsearch client
es = Elasticsearch([{'host': 'localhost', 'port':9200}])

#Initialize Kafka Consumer
consumer = KafkaConsumer(
    'processed_metrics',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

#Send data to Elasticsearch
for message in consumer:
    data = message.value
    es.index(index='gaming-metrics', body=data)
    print(f"Indexed to Elasticsearch: {data}")