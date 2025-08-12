import random
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from faker import Faker

fake = Faker()

# Kafka configuration
kafka_config = {
    'bootstrap.servers': '138.197.228.187:9094',
    'client.id': 'ride-producer'
}

# Schema Registry configuration
schema_registry_conf = {
    'url': 'http://165.227.254.142:8081'
}

# Create Schema Registry client
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Load Avro schema
with open('ride_schema.avsc', 'r') as f:
    ride_schema_str = f.read()

# Define to_dict function
def ride_to_dict(ride, ctx):
    return {
        "ride_id": ride['ride_id'],
        "driver_id": ride['driver_id'],
        "customer_id": ride['customer_id'],
        "start_location": {
            "lat": ride['start_location']['lat'],
            "lon": ride['start_location']['lon']
        },
        "end_location": {
            "lat": ride['end_location']['lat'],
            "lon": ride['end_location']['lon']
        },
        "start_time": ride['start_time'],
        "end_time": ride['end_time'],
        "fare": ride['fare']
    }

# Create Avro serializer
ride_avro_serializer = AvroSerializer(
    schema_registry_client,
    ride_schema_str,
    ride_to_dict
)

# Create key serializer
key_serializer = StringSerializer('utf_8')

# Producer configuration
producer_conf = {
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'key.serializer': key_serializer,
    'value.serializer': ride_avro_serializer,
    'client.id': kafka_config['client.id']
}

# Create Kafka producer
producer = SerializingProducer(producer_conf)

# Assuming customer_ids and driver_ids are populated from the previous producers
customer_ids = [random.randint(1000, 1099) for _ in range(100)]
driver_ids = [random.randint(100, 199) for _ in range(100)]

def produce_ride():
    ride = {
        'ride_id': random.randint(1000, 9999),
        'driver_id': random.choice(driver_ids),
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

def produce_rides():
    topic = 'src-app-ride'
    while True:
        ride_data = produce_ride()
        producer.produce(topic=topic, key=str(ride_data['ride_id']), value=ride_data, on_delivery=delivery_report)
        producer.poll(1)
        time.sleep(1)

if __name__ == '__main__':
    produce_rides()
    producer.flush()  # Ensure all messages are delivered before exiting
