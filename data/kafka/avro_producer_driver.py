import random
import time
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

fake = Faker()

kafka_config = {
    'bootstrap.servers': '138.197.228.187:9094',
    'client.id': 'driver-producer'
}

schema_registry_conf = {
    'url': 'http://165.227.254.142:8081'
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
with open('driver_schema.avsc', 'r') as f:
    driver_schema_str = f.read()

driver_avro_serializer = AvroSerializer(
    schema_registry_client,
    driver_schema_str
)

key_serializer = StringSerializer('utf_8')

producer_conf = {
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'key.serializer': key_serializer,
    'value.serializer': driver_avro_serializer,
    'client.id': kafka_config['client.id']
}

producer = SerializingProducer(producer_conf)

driver_ids = []

def produce_driver():
    driver_id = random.randint(100, 999)
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
    topic = 'src-app-driver-avro'
    for _ in range(num_drivers):
        driver_data = produce_driver()
        driver_ids.append(driver_data['driver_id'])
        producer.produce(topic=topic, key=str(driver_data['driver_id']), value=driver_data, on_delivery=delivery_report)
        producer.poll(1)
        time.sleep(0.1)

if __name__ == '__main__':
    produce_drivers(100)
