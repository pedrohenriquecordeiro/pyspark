# Import necessary libraries for working with Spark and AWS S3
from pyspark.sql import SparkSession

# Create a SparkSession object and enable Delta Lake functionality
spark = SparkSession.builder \
    .appName("") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the S3 bucket paths (input and output)
input_bucket_1 = "s3a://your-input-bucket-name/input-data-1/"
input_bucket_2 = "s3a://your-input-bucket-name/input-data-2/"
delta_table_path = "s3a://your-output-bucket-name/delta-table/"

# Read data from the first S3 bucket (assuming the data is in CSV format)
df1 = spark.read.csv(input_bucket_1, header=True, inferSchema=True)

# Read data from the second S3 bucket (assuming the data is in CSV format)
df2 = spark.read.csv(input_bucket_2, header=True, inferSchema=True)

# Register both dataframes as temporary SQL tables to perform SQL transformations
df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

# SQL query to perform join and several transformations
# - Join table1 and table2 on a common column (e.g., column_id)
# - Select specific columns from both tables
# - Apply transformations like renaming and filtering
transformed_df = spark.sql("""
    SELECT 
        t1.column_id AS id,                    -- Renaming column_id from table1 to id
        t1.column1 AS table1_column1,          -- Selecting column1 from table1
        t2.column2 AS table2_column2,          -- Selecting column2 from table2
        t1.column3 + t2.column4 AS combined_column,  -- Performing a calculation between columns of the two tables
        UPPER(t1.column5) AS column5_upper     -- Uppercase transformation on a column from table1
    FROM 
        table1 t1
        INNER JOIN table2 t2
            ON t1.column_id = t2.column_id     -- Joining the two tables on column_id
    WHERE 
        t1.column1 IS NOT NULL                 -- Filtering out rows where column1 is null in table1
""")

# Show the transformed data to ensure transformations and join were applied correctly
transformed_df.show(5)  # Display the first 5 rows of the transformed dataset

# Write the transformed dataframe to the Delta table
# The "mode" option is set to "overwrite" to replace existing data in the Delta table
transformed_df.write.format("delta").mode("overwrite").save(delta_table_path)

# Stop the Spark session to release resources
spark.stop()
