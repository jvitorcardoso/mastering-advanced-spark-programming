import random
import time
import json
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# Kafka configuration
kafka_config = {
    'bootstrap.servers': '138.197.228.187:9094',
    'client.id': 'ride-producer'
}

producer = Producer(kafka_config)

customer_ids = [random.randint(1000, 1099) for _ in range(100)]

def produce_ride():
    ride = {
        'ride_id': random.randint(1000, 9999),
        'driver_id': random.randint(100, 199),
        'customer_id': random.choice(customer_ids),
        'start_location': {'lat': random.uniform(-90, 90), 'lon': random.uniform(-180, 180)},
        'end_location': {'lat': random.uniform(-90, 90), 'lon': random.uniform(-180, 180)},
        'start_time': int(time.time()),
        'end_time': int(time.time()) + random.randint(300, 3600),
        'fare': round(random.uniform(5, 100), 2)
    }
    return ride

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for ride {msg.key()}: {err}")
    else:
        print(f"Ride {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce_rides(batch_size=10):
    topic = 'src-app-ride-json'
    while True:
        for _ in range(batch_size):
            ride_data = produce_ride()
            producer.produce(topic=topic, key=str(ride_data['ride_id']), value=json.dumps(ride_data).encode('utf-8'), callback=delivery_report)
            producer.poll(1)
        print(f"Produced {batch_size} messages, sleeping for a short period...")
        time.sleep(1)  # Adjust sleep time as needed to control the rate of message production
    producer.flush()

if __name__ == '__main__':
    produce_rides()
