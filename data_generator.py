import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

#Initilize Faker and Kafka producer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#Action types for simulation
action_types = ['move', 'shoot', 'join', 'quit']

#Generate random events
def generate_event():
    return {
        "player_id": fake.uuid4(),
        "game_id": fake.uuid4(),
        "action_type": random.choice(action_types),
        "timestamp": fake.iso8601(),
        "metrics": {
            "reaction_time_ms": random.randint(100, 1000),
            "accuracy": round(random.uniform(0.5, 1.0), 2)
        },
        "location": {
            "lat": round(fake.latitude(), 6),
            "lon": round(fake.longitude(), 6)
        }
    }

#Send events to Kafka
def send_events():
    while True:
        event = generate_event()
        producer.send('raw_events', value=event)
        print(f"Produced: {event}")
        time.sleep(0.1) #Adjust rate of event generation

if __name__ == "__main__":
    send_events()