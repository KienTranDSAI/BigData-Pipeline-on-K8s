# from pyspark.sql import SparkSession
# from datetime import datetime, timedelta
# import time

# spark = SparkSession.builder \
#     .appName("Kafka-Spark Integration") \
#     .getOrCreate()
# stop_time = datetime.now() + timedelta(seconds=10)
# kafka_options = {
#     "kafka.bootstrap.servers": "kafkadev.default.svc.cluster.local:9092",
#     "kafka.sasl.mechanism": "SCRAM-SHA-256",
#     "kafka.security.protocol": "SASL_PLAINTEXT",
#     "kafka.sasl.jaas.config": """org.apache.kafka.common.security.scram.ScramLoginModule required username="user1" password="wxyVevVmgP";""",
#     "startingOffsets": "earliest", 
#     "subscribe": "test"  
# }
# df = spark.readStream.format("kafka").options(**kafka_options).load()
# deserialized_df = df.selectExpr("CAST(value AS STRING)")
# query = deserialized_df.writeStream.outputMode("append").format("console").start()
# query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import time

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Kafka-Spark Integration with Processing") \
    .getOrCreate()

# Kafka connection details
kafka_options = {
    "kafka.bootstrap.servers": "kafkadev.default.svc.cluster.local:9092",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.jaas.config": """org.apache.kafka.common.security.scram.ScramLoginModule required username="user1" password="wxyVevVmgP";""",
    "startingOffsets": "earliest", # Start from the beginning when we consume from kafka
    "subscribe": "test"           # Kafka topic name
}

# Subscribe to Kafka topic and read data as stream
df = spark.readStream.format("kafka").options(**kafka_options).load()
deserialized_df = df.selectExpr("CAST(value AS STRING)")

# Now, let's assume the message is in JSON format, and we need to parse it
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema of the incoming JSON
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Parse the JSON into structured format
parsed_df = deserialized_df.select(from_json(col("value"), schema).alias("data"))

# Extract the 'name' and 'age' fields from the JSON data
processed_df = parsed_df.select("data.name", "data.age")

# Example transformation: Filter out people under the age of 30
filtered_df = processed_df.filter(col("age") >= 30)

# Output the result to the console
query = filtered_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# # Let the stream run for 10 seconds and then stop it
# time.sleep(10)
# query.stop()

# # Stop the Spark session
# spark.stop()
query.awaitTermination()