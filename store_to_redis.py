import redis
import json
from kafka import KafkaConsumer

#Initialize Redis connection
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

#Initialize Kafka Consumer
consumer = KafkaConsumer(
    'processed_metrics',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

#Process and store data in Redis
for message in consumer:
    data = message.value
    game_id = data['data']['game_id']
    redis_client.hmset(f"game:{game_id}:metrics", {
        "avg_reaction_time": data['avg_reaction_time'],
        "actions_per_min": data['actions_per_min']
    })
    print(f"Stored in Redis: {data}")