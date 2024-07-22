import mysql.connector
from confluent_kafka import Producer
import avro.schema
import avro.io
import io
import time

# MySQL connection configuration
mysql_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'kafka'
}

# Kafka configuration
kafka_config = {
    'bootstrap.servers': '',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '',
    'sasl.password': ''
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

def fetch_data(last_read_timestamp):
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor(dictionary=True)
    query = ("SELECT id, name, category, price, last_updated "
             "FROM product "
             "WHERE last_updated > %s")
    cursor.execute(query, (last_read_timestamp,))
    rows = cursor.fetchall()
    cnx.close()
    return rows

def serialize_avro(schema, data):
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(data, encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = Producer(**kafka_config)
last_read_timestamp = "1970-01-01 00:00:00"

while True:
    data = fetch_data(last_read_timestamp)
    for record in data:
        # Convert datetime to string
        record['last_updated'] = record['last_updated'].strftime('%Y-%m-%d %H:%M:%S')
        avro_value = serialize_avro(value_schema, record)
        avro_key = serialize_avro(key_schema, {"id": record['id']})
        producer.produce("product_updates", key=avro_key, value=avro_value, callback=delivery_report)
        last_read_timestamp = max(last_read_timestamp, record['last_updated'])
    producer.flush()
    time.sleep(10)  # Fetch data every 10 seconds
