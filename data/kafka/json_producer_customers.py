import random
import time
import json
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

kafka_config = {
    'bootstrap.servers': '138.197.228.187:9094',
    'client.id': 'customer-producer'
}

producer = Producer(kafka_config)

customer_ids = []

def produce_customer():
    customer_id = random.randint(1, 100)
    customer = {
        'customer_id': customer_id,
        'name': fake.name(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'address': fake.address()
    }
    return customer

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for customer {msg.key()}: {err}")
    else:
        print(f"Customer {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce_customers(num_customers):
    topic = 'src-app-customers-json'
    for _ in range(num_customers):
        customer_data = produce_customer()
        customer_ids.append(customer_data['customer_id'])
        producer.produce(topic=topic, key=str(customer_data['customer_id']), value=json.dumps(customer_data).encode('utf-8'), callback=delivery_report)
        producer.poll(1)
        time.sleep(0.1)
    producer.flush()

if __name__ == '__main__':
    produce_customers(1000)
