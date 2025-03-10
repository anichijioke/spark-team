from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Table Insert with Record ID") \
    .enableHiveSupport() \
    .getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = " tfl_undergroundrecord"
TARGET_TABLE = "tfl_Underground_Result"

# Use the Hive database
# spark.sql(f"USE {HIVE_DB}")

# Load data from the source table
df_source = spark.sql("SELECT * FROM default.tfl_undergroundrecord")

# Add a new 'record_id' column (unique identifier)
df_with_id = df_source.withColumn("record_id", monotonically_increasing_id())

# Write data into the target table
df_with_id.write.mode("append").saveAsTable(TARGET_TABLE)

# print(f"Data successfully inserted from {SOURCE_TABLE} to {TARGET_TABLE} with record_id!")

# Stop Spark session
spark.stop()

