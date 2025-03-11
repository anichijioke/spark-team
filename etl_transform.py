from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, monotonically_increasing_id, regexp_replace, row_number
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
df_source = spark.sql("SELECT * FROM {}.{}".format(HIVE_DB, SOURCE_TABLE))

# Add a new 'record_id' column (unique identifier)
df_with_id = df_source.withColumn("record_id", monotonically_increasing_id())

# Add an "ingestion_timestamp" column
df_with_id = df_with_id.withColumn("ingestion_timestamp", current_timestamp())

# Remove all leading and trailing quotes from the "route" column
df_with_id = df_with_id.withColumn("route", regexp_replace(col("route"), r'^[\'"]+|[\'"]+$', ''))

# Remove NULL values from the route column
df_with_id = df_with_id.filter(col("route").isNotNull())

# Retrieve the maximum existing order_id from the target table
try:
    max_order_id = spark.sql("SELECT MAX(order_id) FROM {}.{}".format(HIVE_DB, TARGET_TABLE)).collect()[0][0]
    if max_order_id is None:
        max_order_id = 0  # If table is empty, start from 1
except:
    max_order_id = 0  # If table doesn't exist, start from 1

# Generate an auto-incremented order_id for new records
window_spec = Window.orderBy(monotonically_increasing_id())
df_with_id = df_with_id.withColumn("order_id", row_number().over(window_spec) + max_order_id)

# Select final columns to match Hive schema
expected_columns = ["order_id", "route", "record_id", "ingestion_timestamp"]
df_with_id = df_with_id.select(*expected_columns)

# Append data into the existing Hive table
df_with_id.write.mode("append").insertInto("{}.{}".format(HIVE_DB, TARGET_TABLE))

# Stop Spark session
spark.stop()
