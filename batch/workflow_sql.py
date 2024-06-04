from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SQL with PySpark") \
    .getOrCreate()

# Create DataFrame
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Register DataFrame as a temporary view
df.createOrReplaceTempView("people")

# Run SQL query
result = spark.sql("SELECT Name, Age FROM people WHERE Age > 30")
result.show()
