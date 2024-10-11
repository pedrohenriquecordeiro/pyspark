# Import necessary libraries for working with Spark and AWS S3
from pyspark.sql import SparkSession

# Create a SparkSession object and enable Delta Lake functionality
spark = SparkSession.builder \
    .appName("S3 to Delta Table with SQL Transformations") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the S3 bucket paths (input and output)
input_bucket = "s3a://your-input-bucket-name/input-data/"
delta_table_path = "s3a://your-output-bucket-name/delta-table/"

# Read data from the input S3 bucket (assuming the data is in CSV format)
df = spark.read.csv(input_bucket, header=True, inferSchema=True)

# Register the dataframe as a temporary SQL table to perform SQL transformations
df.createOrReplaceTempView("temp_table")

# SQL query to perform several transformations
# - Select specific columns
# - Rename columns
# - Filter out rows with null values in a specific column
# - Perform aggregations such as average and sum
transformed_df = spark.sql("""
    SELECT 
        column1 AS new_column1,           -- Rename column1 to new_column1
        column2,                          -- Select column2 as is
        UPPER(column3) AS column3_upper,  -- Convert values in column3 to uppercase
        AVG(column4) OVER() AS avg_column4, -- Calculate the average of column4
        SUM(column5) OVER() AS total_column5 -- Calculate the total sum of column5
    FROM 
        temp_table
    WHERE 
        column1 IS NOT NULL               -- Filter out rows where column1 is null
""")

# Show the transformed data to ensure transformations were applied correctly
transformed_df.show(5)  # Display the first 5 rows of the transformed dataset

# Write the transformed dataframe to the Delta table
# The "mode" option is set to "overwrite" to replace existing data in the Delta table
transformed_df.write.format("delta").mode("overwrite").save(delta_table_path)

# Stop the Spark session to release resources
spark.stop()
