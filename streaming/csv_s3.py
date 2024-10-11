# Import necessary libraries for working with Spark and Delta Lake
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession object and enable Delta Lake functionality
spark = SparkSession.builder \
    .appName("") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the S3 bucket path for reading the CSV files and saving the Delta table
input_csv_path = "s3a://your-input-bucket-name/input-csv/"
delta_table_path = "s3a://your-output-bucket-name/delta-table/"

# Define the schema of the CSV file (if known, otherwise inferSchema can be used)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("column_id", IntegerType(), True),
    StructField("column1", StringType(), True),
    StructField("column2", StringType(), True)
])

# Read the streaming CSV files from S3
csv_stream = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(input_csv_path)

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
query = csv_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "s3a://your-output-bucket-name/checkpoint/") \
    .start()

# Wait for the streaming query to finish (until you terminate it manually)
query.awaitTermination()

# Stop the Spark session when done
spark.stop()
