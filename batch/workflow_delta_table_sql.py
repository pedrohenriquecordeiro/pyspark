from pyspark.sql import SparkSession

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaTableExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Path to Delta table
delta_table_path = "/path/to/delta/table"

# Read Delta table
df = spark.read.format("delta").load(delta_table_path)
df.show()

# Register DataFrame as a temporary view
df.createOrReplaceTempView("delta_table")

# Run SQL query on Delta table
result = spark.sql("SELECT * FROM delta_table WHERE some_column = 'some_value'")
result.show()
