from confluent_kafka import Consumer, KafkaException
import avro.schema
import avro.io
import io
import json
import os

# Kafka configuration
kafka_config = {
    'bootstrap.servers': '',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '',
    'sasl.password': '',
    'group.id': 'product_consumers',
    'auto.offset.reset': 'earliest'
}

# Avro value schema
value_schema_str = """
{
  "type": "record",
  "name": "Product",
  "namespace": "product.avro",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "category", "type": "string"},
    {"name": "price", "type": "float"},
    {"name": "last_updated", "type": "string"}
  ]
}
"""
value_schema = avro.schema.parse(value_schema_str)

# Avro key schema
key_schema_str = """
{
  "type": "record",
  "name": "ProductKey",
  "namespace": "product.avro",
  "fields": [
    {"name": "id", "type": "int"}
  ]
}
"""
key_schema = avro.schema.parse(key_schema_str)

consumer = Consumer(**kafka_config)
consumer.subscribe(['product_updates'])

def deserialize_avro(schema, msg):
    bytes_reader = io.BytesIO(msg)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    data = reader.read(decoder)
    return data

def transform_data(data):
    data['category'] = data['category'].upper()
    if data['category'] == 'SPECIFIC_CATEGORY':
        data['price'] *= 0.9  # Apply a 10% discount
    return data

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            raise KafkaException(msg.error())
    key = deserialize_avro(key_schema, msg.key())
    value = deserialize_avro(value_schema, msg.value())
    transformed_record = transform_data(value)
    file_path = f"product_{msg.partition()}.json"
    with open(file_path, 'a') as file:
        json_str = json.dumps(transformed_record)
        file.write(json_str + '\n')

consumer.close()
