from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Word Count Example") \
    .getOrCreate()

# Read input text file
input_file = "/tmp/text.txt"  # Can be a local file path or HDFS path
text_file = spark.read.text(input_file)

# Perform word count using DataFrame API
words = text_file.select(explode(split(text_file['value'], ' ')).alias('word'))
word_counts = words.groupBy('word').count()

# Show the result
word_counts.show()

# Stop the SparkSession
spark.stop()
