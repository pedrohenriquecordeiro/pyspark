from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Create a SparkSession
spark = SparkSession.builder \
    .appName("foreachBatchExample") \
    .getOrCreate()

# Define a function to process each micro-batch
def process_batch(df, batch_id):
    # Print the number of rows in the batch
    print(f"Batch ID: {batch_id}, Number of Rows: {df.count()}")

    # Optionally, perform further processing on the DataFrame
    # For example, filter or aggregate data
    filtered_df = df.filter(col("value") > 10)  # Filter rows with value > 10
    count_df = filtered_df.groupBy("key").count()  # Count occurrences per key

    # Print or write the processed data (replace with your desired sink)
    print(count_df.show())

# Create a streaming DataFrame (replace with your source)
streaming_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the incoming data into key-value pairs (adjust based on your data format)
key_value_df = streaming_df.select(col("value").cast("string").alias("key"), col("value"))

# Apply the foreachBatch transformation
query = key_value_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Start the query and await termination
query.awaitTermination()
