from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()

# Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "your.kafka.broker:9093",  # Replace with your Kafka broker address
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"{'wxyVevVmgP'}\";"
}

# Read from Kafka topic (replace 'your_topic' with your actual topic name)
kafka_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .option("subscribe", "test") \
    .load()

# Kafka data comes as a key-value pair, so we need to decode the value (typically in byte format)
# Assuming the Kafka messages are in plain text (utf-8), if it's in JSON or other formats, you'd need additional parsing
kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Display the data in the console for debugging (or process it further)
query = kafka_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the stream to process
query.awaitTermination()
