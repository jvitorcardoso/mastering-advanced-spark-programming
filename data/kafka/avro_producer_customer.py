import json
import random
import time
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

# Kafka and Schema Registry configuration
kafka_config = {
    'bootstrap.servers': '138.197.228.187:9094',
    'client.id': 'ride-producer'
}

schema_registry_conf = {
    'url': 'http://165.227.254.142:8081'
}

# Load the Avro schemas
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
with open('ride_schema.avsc', 'r') as f:
    ride_schema_str = f.read()

with open('customer_schema.avsc', 'r') as f:
    customer_schema_str = f.read()

ride_avro_serializer = AvroSerializer(
    schema_registry_client,
    ride_schema_str
)

customer_avro_serializer = AvroSerializer(
    schema_registry_client,
    customer_schema_str
)

key_serializer = StringSerializer('utf_8')

producer_conf = {
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'key.serializer': key_serializer,
    'value.serializer': ride_avro_serializer,
    'client.id': kafka_config['client.id']
}

# Create SerializingProducer instance
producer = SerializingProducer(producer_conf)

customer_producer_conf = {
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'key.serializer': key_serializer,
    'value.serializer': customer_avro_serializer,
    'client.id': kafka_config['client.id']
}

customer_producer = SerializingProducer(customer_producer_conf)

# List to hold customer IDs
customer_ids = []

# Initialize Faker
fake = Faker()

# Function to produce customer data
def produce_customer():
    customer_id = random.randint(1000, 9999)
    customer = {
        'customer_id': customer_id,
        'name': fake.name(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'address': fake.address()
    }
    return customer

# Function to produce ride data
def produce_ride():
    ride = {
        'ride_id': random.randint(1000, 9999),
        'driver_id': random.randint(100, 999),
        'passenger_id': random.choice(customer_ids),  # Select from existing customer IDs
        'start_location': {'lat': random.uniform(-90, 90), 'lon': random.uniform(-180, 180)},
        'end_location': {'lat': random.uniform(-90, 90), 'lon': random.uniform(-180, 180)},
        'start_time': int(time.time()),
        'end_time': int(time.time()) + random.randint(300, 3600),  # Ride duration between 5 minutes to 1 hour
        'fare': round(random.uniform(5, 100), 2)  # Random fare between $5 and $100
    }
    return ride

# Delivery report callback function
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Produce customer data to Kafka topic 'customers'
def produce_customers(num_customers):
    topic = 'customers'
    for _ in range(num_customers):
        customer_data = produce_customer()
        customer_ids.append(customer_data['customer_id'])
        customer_producer.produce(topic=topic, key=str(customer_data['customer_id']), value=customer_data, on_delivery=delivery_report)
        customer_producer.poll(1)
        time.sleep(0.1)  # Short delay between producing customer data

# Produce ride data to Kafka topic 'rides'
def produce_rides():
    topic = 'rides'
    while True:
        ride_data = produce_ride()
        producer.produce(topic=topic, key=str(ride_data['ride_id']), value=ride_data, on_delivery=delivery_report)
        producer.poll(1)
        time.sleep(1)  # Wait for 1 second before sending the next ride data

def main():
    # Produce customers first
    produce_customers(100)  # Produce 100 customers

    # Produce rides continuously
    produce_rides()

if __name__ == '__main__':
    main()
