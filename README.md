# Real-Time-Data-Processing-with-Confluent-Kafka-MySQL-and-Avro
We will build a Kafka producer and a consumer group that work with a MySQL database, Avro serialization, and multi-partition Kafka topics. The producer will fetch incremental data from a MySQL table and write Avro serialized data into a Kafka topic. The consumers will deserialize this data and append it to separate JSON files.
<img width="717" alt="image" src="https://github.com/user-attachments/assets/9c3347f1-6ee1-40a3-8722-6f672bb1339a">


# Tools Required:
● Python 3.7 or later
● Confluent Kafka Python client
● MySQL Database
● Apache Avro
● A suitable IDE for coding (e.g., PyCharm, Visual Studio Code)

# Background:
We are working in a fictitious e-commerce company called "BuyOnline"
which has a MySQL database that stores product information such as
product ID, name, category, price, and updated timestamp. The
database gets updated frequently with new products and changes in
product information. The company wants to build a real-time system to
stream these updates incrementally to a downstream system for
real-time analytics and business intelligence.

# Steps:
Step 1: MySQL Table Setup
Create a table named 'product' in MySQL database with the following
columns:
➔ id - INT (Primary Key)
➔ name - VARCHAR
➔ category - VARCHAR
Grow Data Skills
➔ price - FLOAT
➔ last_updated - TIMESTAMP
<img width="639" alt="image" src="https://github.com/user-attachments/assets/33d6e98b-cd6b-4e6c-88fc-651b7675e796">


# Step 2: Kafka Producer
● Write a Kafka producer in Python that uses a MySQL connector to
fetch data from the MySQL table.
● In your producer code, maintain a record of the last read
timestamp. Each time you fetch data, use a SQL query to get
records where the last_updated timestamp is greater than the last
read timestamp.
● Serialize the data into Avro format and publish the data to a Kafka
topic named "product_updates". This topic should be configured
with 10 partitions.
● Use the product ID as the key when producing messages. This will
ensure that all updates for the same product end up in the same
partition.
● Update the last read timestamp after each successful fetch.

# Step 3: Kafka Consumer Group and Data Transformation
● Write a Kafka consumer in Python and set it up as a consumer
group of 5 consumers.
● Each consumer should read data from the "product_updates"
topic.
● Deserialize the Avro data back into a Python object.
● Implement data transformation logic. For example:
● Change the category column to uppercase.
● Apply some business logic to update the price column, such as
applying a discount if a particular product falls under a specific
category.
# Step 4: Writing Transformed Data to JSON Files
● Each consumer should convert the transformed Python object into
a JSON string and append the JSON string to a separate JSON
file. Make sure to open the file in append mode.
● Also, ensure each new record is written in a new line for ease of
reading.
● Close the file after writing to make sure all data is saved properly.
Grow Data Skills
Deliverables:
● The source code for the Kafka producer and consumer group in
Python.
● SQL queries used for incremental data fetch from MySQL.
● Avro schema used for data serialization.
● Data transformation logic.
● The resulting JSON files with appended records.
● Documentation explaining how to run and test the system.
● Screenshots demonstrating the successful running of your
application.
