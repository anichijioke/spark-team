from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, monotonically_increasing_id, regexp_replace

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Table Insert with Record ID") \
    .enableHiveSupport() \
    .getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = "tfl_undergroundrecord"
TARGET_TABLE = "tfl_underground_result"

# Load data from the source table
df_source = spark.sql("SELECT * FROM {}.{}".format(HIVE_DB, SOURCE_TABLE))

# Add a new 'record_id' column (unique identifier)
df_with_id = df_source.withColumn("record_id", monotonically_increasing_id())

# Add an "ingestion_timestamp" column
df_with_id = df_with_id.withColumn("ingestion_timestamp", current_timestamp())

# Remove all leading and trailing quotes from the "route" column
df_with_id = df_with_id.withColumn("route", regexp_replace(col("route"), r'^[\'"]+|[\'"]+$', ''))

# Remove NULL values from the route column
df_with_id = df_with_id.filter(col("route").isNotNull())

# Append data into the existing Hive table
df_with_id.write.mode("append").insertInto("{}.{}".format(HIVE_DB, TARGET_TABLE))

# Stop Spark session
spark.stop()
