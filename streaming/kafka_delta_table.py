# Import necessary libraries for working with Spark, Kafka, and Delta Lake
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession object and enable Delta Lake functionality
spark = SparkSession.builder \
    .appName("") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the S3 bucket path for the Delta table
delta_table_path = "s3a://your-output-bucket-name/delta-table/"

# Define the Kafka broker and topic
kafka_brokers = "your-kafka-broker:9092"
kafka_topic = "your-topic-name"

# Define the schema of the Kafka message
schema = StructType([
    StructField("column_id", IntegerType(), True),
    StructField("column1", StringType(), True),
    StructField("column2", StringType(), True)
])

# Read streaming data from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize the Kafka message (assuming the message is in JSON format)
kafka_value_df = kafka_stream.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")  # Flatten the nested structure

# Define the transformation function to be applied to each micro-batch
def process_batch(batch_df, batch_id):
    # Example transformation: filter rows where column1 is not null and column_id is greater than 10
    transformed_df = batch_df.filter((col("column1").isNotNull()) & (col("column_id") > 10))

    # Write the transformed dataframe to the Delta table in S3
    transformed_df.write \
        .format("delta") \
        .mode("append") \
        .save(delta_table_path)

# Apply the transformation using foreachBatch
query = kafka_value_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "s3a://your-output-bucket-name/checkpoint/") \
    .start()

# Wait for the streaming query to finish (until you terminate it manually)
query.awaitTermination()

# Stop the Spark session when done
spark.stop()
