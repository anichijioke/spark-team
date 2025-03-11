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

# Check if the target table exists in Hive
table_check = spark.sql("SHOW TABLES IN {}".format(HIVE_DB)).filter(col("tableName") == TARGET_TABLE).count()

# If the table does not exist, create it
if table_check == 0:
    spark.sql("""
        CREATE TABLE {}.{} (
            route STRING,
            record_id BIGINT,
            ingestion_timestamp TIMESTAMP
        ) STORED AS PARQUET
    """.format(HIVE_DB, TARGET_TABLE))

# Append data instead of overwriting
df_with_id.write.mode("append").format("hive").saveAsTable("{}.{}".format(HIVE_DB, TARGET_TABLE))

# Stop Spark session
spark.stop()
