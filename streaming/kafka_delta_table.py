# Import necessary libraries for working with Spark, Delta, and Kafka
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession object with Delta and Kafka support
spark = SparkSession.builder \
    .appName("") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define Kafka broker and topic
kafka_broker = "your.kafka.broker:9092"
kafka_topic = "your-kafka-topic"

# Define the Delta table path in S3 bucket
delta_table_path = "s3a://your-output-bucket-name/delta-streaming-table/"

# Read streaming data from Kafka topic
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka data from binary to string for key and value
kafka_stream_df = kafka_stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define a function to perform transformations using `foreach`
def process_row(row):
    # Split the value column by some delimiter, e.g., comma
    values = row["value"].split(",")
    
    # Assume values[0] corresponds to a column called 'id', values[1] to 'name', etc.
    id = values[0]
    name = values[1].upper()  # Example transformation: uppercase the name
    value = float(values[2]) * 2  # Example transformation: multiply a numerical value by 2
    
    # Return a tuple 
    return (id, name, value)

# Apply transformations using `foreach` on each row
transformed_stream_df = kafka_stream_df.rdd.map(lambda row: process_row(row)).toDF(["id", "name", "value"])

# Write the transformed stream to Delta table in S3
delta_write_stream = transformed_stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://your-output-bucket-name/checkpoint/") \
    .start(delta_table_path)

# Wait for the streaming query to terminate
delta_write_stream.awaitTermination()
