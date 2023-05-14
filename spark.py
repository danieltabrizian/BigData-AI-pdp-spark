from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Join and Sort comics by scale from dc and marvel worlds") \
    .getOrCreate()

# Read the CSV files into separate DataFrames
csv1 = spark.read.csv("dc-wikia-data.csv", header=True, inferSchema=True)
csv2 = spark.read.csv("marvel-wikia-data.csv", header=True, inferSchema=True)

# Concatenate the two DataFrames into one
combined_df = csv1.union(csv2)

# Clean up the dataset by removing any null or empty values
clean_df = combined_df.na.drop()

# Convert the "APPEARANCES" column to integer data type to make sure it can be sorted on
clean_df = clean_df.withColumn("APPEARANCES", col("APPEARANCES").cast(IntegerType()))

# Sort the resulting DataFrame based on the "APPEARANCES" column to get the biggest comic
sorted_df = clean_df.sort(col("APPEARANCES").desc())

# Show the result
sorted_df.show()

# Stop the Spark session
spark.stop()
