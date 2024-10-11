# Import necessary libraries for working with Spark and AWS S3
from pyspark.sql import SparkSession

# Create a SparkSession object and enable Delta Lake functionality
spark = SparkSession.builder \
    .appName("S3 to Delta Table") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the S3 bucket paths (input and output)
input_bucket = "s3a://your-input-bucket-name/input-data/"
delta_table_path = "s3a://your-output-bucket-name/delta-table/"

# Read data from the input S3 bucket (assuming the data is in CSV format)
df = spark.read.csv(input_bucket, header=True, inferSchema=True)

# Show a sample of the data to ensure it's been read correctly
df.show(5)  # Display the first 5 rows of the dataset

# Perform any transformations on the dataframe if necessary (optional)
# For example, let's filter out rows where a certain column is null
df_filtered = df.filter(df['column_name'].isNotNull())

# Write the filtered dataframe to the Delta table
# The "mode" option is set to "overwrite" to replace existing data in the Delta table
df_filtered.write.format("delta").mode("overwrite").save(delta_table_path)

# Stop the Spark session to release resources
spark.stop()
