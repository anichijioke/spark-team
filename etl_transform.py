from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark Session with Hive Support
spark = SparkSession.builder \
    .appName("Hive ETL Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Source and target tables
source_table = "default.tfl_undergroundrecord"
target_table = "default.tfl_underground_result"

# Step 1: Load data from the source Hive table
print("Step 1: Reading data from Hive table")
df_source = spark.sql("SELECT * FROM {}".format(source_table))

# Step 2: Check if the table exists and get the last processed record_id
try:
    print("Step 2: Fetching last processed record_id from target table")
    last_recordid = spark.sql(f"SELECT MAX(record_id) FROM {target_table}").collect()[0][0]
except Exception as e:
    last_recordid = None

# If table doesn't exist or it's the first load, set the starting record_id to 0
if last_recordid is None:
    last_recordid = 0

print(f"Last processed record_id: {last_recordid}")

# Step 3: Filter new records that haven't been processed
df_new = df_source.filter(col("record_id") > last_recordid)

# If no new records, exit early
if df_new.isEmpty():
    print("No new records to process. Exiting...")
    spark.stop()
    exit()

# Step 4: Transformations
print("Step 3: Performing transformations...")

# Window specification based on original record_id
windowSpec = Window.orderBy("record_id")

# Assign a new record_id sequentially, continuing from the last_recordid
df_transformed = df_new.withColumn("record_id", F.row_number().over(windowSpec) + last_recordid)

# Remove records where critical fields are NULL
df_transformed = df_transformed.filter(col("route").isNotNull())

# Add an ingestion timestamp
df_transformed = df_transformed.withColumn("ingestion_timestamp", current_timestamp())

# Ensure valid timedetails (avoid filtering useful data accidentally)
invalid_values = [
    'timedetails', 'timedetails String', 'status String', 'route String',
    'reason String', 'line String', 'id int', 'delay_time String',
    'STORED AS TEXTFILE', 'ROW FORMAT DELIMITED',
    'LOCATION /tmp/big_datajan2025/TFL/TFL_UndergroundRecord',
    'LINES TERMINATED BY \\n', 'FIELD TERMINATED BY ', 
    'CREATE EXTERNAL TABLE default.tfl_ugrFullScoop ('
]

df_transformed = df_transformed.filter(~col("timedetails").isin(invalid_values))
df_transformed = df_transformed.filter(col("timedetails").isNotNull())

# Show transformed data for debugging
df_transformed.show()

# Step 5: Insert transformed data into target table (append to prevent overwriting)
print("Step 4: Writing transformed data to Hive table")
df_transformed.write.mode("append").format("hive").insertInto(target_table)

print("ETL Process Completed Successfully!")

# Stop Spark Session
spark.stop()
