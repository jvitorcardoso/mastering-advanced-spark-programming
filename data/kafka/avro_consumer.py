import json
from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer

# Kafka and Schema Registry configuration
broker = '138.197.228.187:9094'  # Change this to your Kafka broker
group_id = 'python-consumer-group'
offset_reset = 'earliest'
schema_registry_url = 'http://165.227.254.142:8081'  # Change this to your Schema Registry URL
topic = 'src-app-driver'

# Validate URL
def validate_url(url):
    import re
    url = url.strip()
    if not re.match(r'http[s]?://', url):
        raise ValueError(f"Invalid schema registry URL: {url}")
    return url

schema_registry_url = validate_url(schema_registry_url)

# Schema Registry Client
schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})

# Get Avro schema for the topic
schema_id = schema_registry_client.get_latest_version('src-app-driver-value').schema_id
avro_schema_str = schema_registry_client.get_schema(schema_id).schema_str

# Deserializers
avro_deserializer = AvroDeserializer(schema_registry_client, avro_schema_str)
string_deserializer = StringDeserializer('utf_8')

# Consumer configuration
consumer_config = {
    'bootstrap.servers': broker,
    'group.id': group_id,
    'auto.offset.reset': offset_reset,
    'key.deserializer': string_deserializer,
    'value.deserializer': avro_deserializer
}

# Create DeserializingConsumer instance
consumer = DeserializingConsumer(consumer_config)
consumer.subscribe([topic])

# Consume messages
try:
    while True:
        msg = consumer.poll(1.0)  # Short polling timeout
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                continue
            else:
                raise KafkaError(msg.error())  # Or handle the error differently
        else:
            try:
                print(f"Consumed ride: {json.dumps(msg.value(), indent=2)}")
            except Exception as e:
                print(f"Message deserialization failed: {e}")
                continue
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
