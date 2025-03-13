from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, monotonically_increasing_id
from pyspark.sql.types import IntegerType
import logging

# Initialize Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Table Insert with Auto Increment Record ID") \
    .enableHiveSupport() \
    .getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = "tfl_undergroundrecord"
TARGET_TABLE = "tfl_underground_result_n"

logger.info("Loading data from source table: %s.%s", HIVE_DB, SOURCE_TABLE)

# Load data from the source table
df_source = spark.sql("SELECT * FROM {}.{}".format(HIVE_DB, SOURCE_TABLE))

# Add an "ingestion_timestamp" column
df_transformed = df_source.withColumn("ingestion_timestamp", current_timestamp())

# Clean "route", "delay_time", and "reason" columns by removing all quotes
for col_name in ["route", "delay_time", "reason"]:  # Added "reason" column to the list
    df_transformed = df_transformed.withColumn(col_name, regexp_replace(col(col_name), r'["\']+', ''))

# Remove NULL values from the "route" column
df_transformed = df_transformed.filter(col("route").isNotNull())

# Ensure target table exists before querying max(record_id)
try:
    max_record_id_query = "SELECT MAX(record_id) FROM {}.{}".format(HIVE_DB, TARGET_TABLE)
    max_record_id = spark.sql(max_record_id_query).collect()[0][0] or 0
    logger.info("Max existing record_id: %d", max_record_id)
except:
    logger.warning("Table %s.%s does not exist. Starting record_id from 1.", HIVE_DB, TARGET_TABLE)
    max_record_id = 0

# Generate a unique record_id using monotonically_increasing_id
df_transformed = df_transformed.withColumn("record_id", (monotonically_increasing_id() + max_record_id).cast(IntegerType()))

# Ensure column order matches the Hive table
expected_columns = ["record_id", "timedetails", "line", "status", "reason", "delay_time", "route", "ingestion_timestamp"]
df_final = df_transformed.select(*expected_columns)

# Sort data by "timedetails" in descending order to make the most recent data appear first
df_sorted = df_final.orderBy(col("timedetails").desc())

logger.info("Writing transformed and sorted data to Hive table: %s.%s", HIVE_DB, TARGET_TABLE)

# Append data into the existing Hive table, ensuring no duplication
df_sorted.createOrReplaceTempView("new_data")

# Use a left anti join to avoid duplicating existing records based on record_id
df_existing = spark.sql("SELECT * FROM {}.{}".format(HIVE_DB, TARGET_TABLE))
df_unique = df_sorted.join(df_existing, "record_id", "left_anti")

# Write only new data
df_unique.write.mode("append").insertInto("{}.{}".format(HIVE_DB, TARGET_TABLE))

logger.info("Data successfully written to Hive. Closing Spark session.")

# Stop Spark session
spark.stop()
