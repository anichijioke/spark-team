from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, monotonically_increasing_id, regexp_replace

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Table Insert with Record ID") \
    .enableHiveSupport() \
    .getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = " tfl_undergroundrecord"
TARGET_TABLE = "tfl_underground_result"

# Use the Hive database
# spark.sql(f"USE {HIVE_DB}")

# Load data from the source table
df_source = spark.sql("SELECT * FROM default.tfl_undergroundrecord")

# Add a new 'record_id' column (unique identifier)
df_with_id = df_source.withColumn("record_id", monotonically_increasing_id())

# 3. Adding an "ingestion_timestamp" column
df_with_id = df_with_id.withColumn("ingestion_timestamp", current_timestamp())

# Remove quotes from the "route" column
df_with_id = df_with_id.withColumn("route", regexp_replace(col("route"), r'["\']', ''))

# Write data into the target table
#df_with_id.write.mode("append").saveAsTable(TARGET_TABLE)
df_with_id.write.mode("append").format("hive").saveAsTable(TARGET_TABLE)


# print(f"Data successfully inserted from {SOURCE_TABLE} to {TARGET_TABLE} with record_id!")

# Stop Spark session
spark.stop()

