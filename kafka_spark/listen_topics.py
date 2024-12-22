from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import psycopg2

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
    "startingOffsets": "earliest"  # Start from the beginning when we consume from Kafka
}

# Define schema for the 'test' topic (users data)
test_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Define schema for the 'movies' topic (movie data)
movies_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("movie_name", StringType(), True)
])

# Read stream from Kafka (test topic)
test_df = spark.readStream.format("kafka").options(**kafka_options).option("subscribe", "test").load()

# Deserialize the Kafka message value (as a string) for 'test' topic
test_deserialized_df = test_df.select(from_json(col("value").cast("string"), test_schema).alias("data"))

# Flatten the schema (extract fields) for 'test' topic
test_flattened_df = test_deserialized_df.select("data.id", "data.name", "data.age")

# Read stream from Kafka (movies topic)
movies_df = spark.readStream.format("kafka").options(**kafka_options).option("subscribe", "movies").load()

# Deserialize the Kafka message value (as a string) for 'movies' topic
movies_deserialized_df = movies_df.select(from_json(col("value").cast("string"), movies_schema).alias("data"))

# Flatten the schema (extract fields) for 'movies' topic
movies_flattened_df = movies_deserialized_df.select("data.id", "data.movie_name")

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

# Function to insert data into PostgreSQL (users table for 'test' topic)
def insert_users_into_postgres(df, batch_id):
    conn = get_postgresql_connection()
    if conn:
        cursor = conn.cursor()
        try:
            # Iterate through each row in the dataframe and insert into PostgreSQL (users)
            for row in df.collect():
                id = row['id']
                name = row['name']
                age = row['age']
                query = f"INSERT INTO users (id, name, age) VALUES (%s, %s, %s);"
                cursor.execute(query, (id, name, age))
            conn.commit()
            print(f"Batch {batch_id}: Data inserted into users.")
        except (psycopg2.Error, Exception) as e:
            print(f"Error inserting data into users: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

# Function to insert data into PostgreSQL (movies table for 'movies' topic)
def insert_movies_into_postgres(df, batch_id):
    conn = get_postgresql_connection()
    if conn:
        cursor = conn.cursor()
        try:
            # Iterate through each row in the dataframe and insert into PostgreSQL (movies)
            for row in df.collect():
                id = row['id']
                movie_name = row['movie_name']
                query = f"INSERT INTO movies (id, movie_name) VALUES (%s, %s);"
                cursor.execute(query, (id, movie_name))
            conn.commit()
            print(f"Batch {batch_id}: Data inserted into movies.")
        except (psycopg2.Error, Exception) as e:
            print(f"Error inserting data into movies: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

# Write the processed data from 'test' topic to PostgreSQL
test_query = test_flattened_df.writeStream \
    .foreachBatch(insert_users_into_postgres) \
    .outputMode("append") \
    .start()

# Write the processed data from 'movies' topic to PostgreSQL
movies_query = movies_flattened_df.writeStream \
    .foreachBatch(insert_movies_into_postgres) \
    .outputMode("append") \
    .start()

test_query.awaitTermination()
movies_query.awaitTermination()
