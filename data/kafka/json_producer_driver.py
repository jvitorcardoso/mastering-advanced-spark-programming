import random
import time
import json
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

kafka_config = {
    'bootstrap.servers': '138.197.228.187:9094',
    'client.id': 'driver-producer'
}

producer = Producer(kafka_config)

driver_ids = []

def produce_driver():
    driver_id = random.randint(1, 100)
    driver = {
        'driver_id': driver_id,
        'name': fake.name(),
        'license_number': fake.license_plate(),
        'rating': round(random.uniform(3.0, 5.0), 2)
    }
    return driver

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for driver {msg.key()}: {err}")
    else:
        print(f"Driver {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce_drivers(num_drivers):
    topic = 'src-app-drivers-json'
    for _ in range(num_drivers):
        driver_data = produce_driver()
        driver_ids.append(driver_data['driver_id'])
        producer.produce(topic=topic, key=str(driver_data['driver_id']), value=json.dumps(driver_data).encode('utf-8'), callback=delivery_report)
        producer.poll(1)
        time.sleep(0.1)
    producer.flush()

if __name__ == '__main__':
    produce_drivers(1000)
