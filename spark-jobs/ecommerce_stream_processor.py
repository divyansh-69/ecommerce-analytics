import os
import sys

# Set environment variables for Windows compatibility
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

print("Starting Spark application...")

# Initialize Spark Session with minimal configuration for Windows
spark = SparkSession.builder \
    .appName("EcommerceStreamProcessor") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark Session Created Successfully!")
print(f"Spark Version: {spark.version}")

# Simple schema for incoming events
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True)
])

print("Schema defined successfully!")

# Test Kafka connectivity first
try:
    from kafka import KafkaConsumer
    print("Testing Kafka connectivity...")
    consumer = KafkaConsumer(
        'ecommerce-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    # Test for a few messages
    print("Kafka connection successful! Testing message consumption...")
    message_count = 0
    for message in consumer:
        print(f"Received message: {message.value['event_type']} - {message.value['product_name']}")
        message_count += 1
        if message_count >= 3:
            break
    
    consumer.close()
    print(f"Successfully received {message_count} messages from Kafka!")
    
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    print("Make sure Kafka is running and topics are created")

print("\nKafka test completed. Spark session is ready for streaming!")
print("Press Ctrl+C to stop")

try:
    # Keep the session alive
    input("Press Enter to stop the Spark session...")
except KeyboardInterrupt:
    print("\nStopping Spark session...")

spark.stop()
print("Spark session stopped successfully!")