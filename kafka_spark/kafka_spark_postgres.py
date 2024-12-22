from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import psycopg2
import time

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Kafka-Spark-PostgreSQL") \
    .getOrCreate()

# Kafka connection details
kafka_options = {
    "kafka.bootstrap.servers": "kafkadev.default.svc.cluster.local:9092",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.jaas.config": """org.apache.kafka.common.security.scram.ScramLoginModule required username="user1" password="wxyVevVmgP";""",
    "startingOffsets": "earliest",  # Start from the beginning when we consume from kafka
    "subscribe": "test"            # Kafka topic name
}

# Define schema for the expected message structure
schema = StructType([
    StructField("id", IntegerType(), True),  # Add 'id' field to the schema
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Read stream from Kafka
df = spark.readStream.format("kafka").options(**kafka_options).load()

# Deserialize the Kafka message value (as a string)
deserialized_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Flatten the schema (i.e., extract fields from the "data" struct)
flattened_df = deserialized_df.select("data.id", "data.name", "data.age")

# Filter the data where age > 25
filtered_df = flattened_df.filter(col("age") > 25)

# PostgreSQL connection function
def get_postgresql_connection():
    # PostgreSQL connection configuration
    postgres_config = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'EaKxUvUAec',
        'host': 'psql-test-postgresql',  # Replace with your PostgreSQL service name
        'port': '5432'
    }
    try:
        conn = psycopg2.connect(**postgres_config)
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# Function to insert data into PostgreSQL
def insert_into_postgres(df, batch_id):
    conn = get_postgresql_connection()
    if conn:
        cursor = conn.cursor()
        try:
            # Iterate through each row in the dataframe and insert into PostgreSQL
            for row in df.collect():
                id = row['id']  # Access the id field
                name = row['name']
                age = row['age']
                
                # Prepare SQL query to insert into test_table
                query = f"INSERT INTO users (id, name, age) VALUES (%s, %s, %s);"
                cursor.execute(query, (id, name, age))
                
            # Commit the transaction
            conn.commit()
            print(f"Batch {batch_id}: Data inserted successfully into users.")
        except (psycopg2.Error, Exception) as e:
            print(f"Error inserting data into PostgreSQL: {e}")
            conn.rollback()  # Rollback if there is an error
        finally:
            cursor.close()
            conn.close()

# Write the processed data to PostgreSQL
query = filtered_df.writeStream \
    .foreachBatch(insert_into_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
