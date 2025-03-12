from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, row_number
from pyspark.sql.window import Window

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Table Insert with Auto Increment Order ID") \
    .enableHiveSupport() \
    .getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = "tfl_undergroundrecord"
TARGET_TABLE = "tfl_underground_result"

# Load data from the source table
df_source = spark.sql(f"SELECT * FROM {HIVE_DB}.{SOURCE_TABLE}")

# Add an "ingestion_timestamp" column
df_transformed = df_source.withColumn("ingestion_timestamp", current_timestamp())

# Remove all leading and trailing quotes from the "route" column
df_transformed = df_transformed.withColumn("route", regexp_replace(col("route"), r'^[\'"]+|[\'"]+$', ''))

# Remove NULL values from the route column
df_transformed = df_transformed.filter(col("route").isNotNull())

# Retrieve the maximum existing record_id from the target table
try:
    max_order_id = spark.sql(f"SELECT MAX(record_id) FROM {HIVE_DB}.{TARGET_TABLE}").collect()[0][0]
    if max_order_id is None:
        max_order_id = 0  # If table is empty, start from 1
except:
    max_order_id = 0  # If table doesn't exist, start from 1

# Generate an auto-incremented record_id based on row_number()
window_spec = Window.orderBy("ingestion_timestamp")  # Order by timestamp for sequential assignment
df_transformed = df_transformed.withColumn("record_id", row_number().over(window_spec) + max_order_id)

# Ensure column order matches Hive table
expected_columns = ["record_id", "timedetails", "line", "status", "reason", "delay_time", "route", "ingestion_timestamp"]
df_final = df_transformed.select(*expected_columns)

# Append data into the existing Hive table
df_with_id.write.mode("append").insertInto("{}.{}".format(HIVE_DB, TARGET_TABLE))

# Stop Spark session
spark.stop()
