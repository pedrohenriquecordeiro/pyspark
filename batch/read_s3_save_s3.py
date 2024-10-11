# Import necessary libraries for working with Spark and AWS S3
from pyspark.sql import SparkSession

# Create a SparkSession object to initialize PySpark
spark = SparkSession.builder \
    .appName("S3 Read and Write") \
    .getOrCreate()

# Define the S3 bucket paths (input and output)
input_bucket = "s3a://your-input-bucket-name/input-data/"
output_bucket = "s3a://your-output-bucket-name/output-data/"

# Read data from the input S3 bucket (assuming the data is in CSV format)
# Replace "csv" with the appropriate format if needed (e.g., "parquet" or "json")
df = spark.read.csv(input_bucket, header=True, inferSchema=True)

# Show a sample of the data to ensure it's been read correctly
df.show(5)  # Display the first 5 rows of the dataset

# Perform any transformations on the dataframe if necessary (optional)
# For example, let's filter out rows where a certain column is null
df_filtered = df.filter(df['column_name'].isNotNull())

# Write the filtered dataframe to the output S3 bucket in Parquet format
# The "mode" option is set to "overwrite" to replace existing data in the output bucket
df_filtered.write.mode("overwrite").parquet(output_bucket)

# Stop the Spark session to release resources
spark.stop()
