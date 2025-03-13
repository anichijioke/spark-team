from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark Session with Hive Support
spark = SparkSession.builder \
    .appName("Hive ETL Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define source and target tables
source_table = "default.tfl_undergroundrecord"
target_table = "default.tfl_underground_result"

# Step 1: Load data from the source Hive table
print("Step 1: Reading data from Hive table...")
df_source = spark.sql("SELECT * FROM {}".format(source_table))

# Print available columns for debugging
print("Columns in source table:", df_source.columns)

# Step 2: Check if target table exists and get the last processed record_id
try:
    print("Step 2: Fetching last processed record_id from target table...")
    last_recordid = spark.sql("SELECT MAX(record_id) FROM {}".format(target_table)).collect()[0][0]
except Exception as e:
    last_recordid = None

# If the table is empty or doesn't exist, set last_recordid to 0
if last_recordid is None:
    last_recordid = 0

print("Last processed record_id: {}".format(last_recordid))

# Step 3: Generate record_id dynamically using row_number()
print("Step 3: Generating record_id dynamically...")

# Define a window specification based on a stable sorting column
# If your source table has a timestamp column, use it instead of "timedetails"
windowSpec = Window.orderBy("timedetails")

df_source = df_source.withColumn("record_id", row_number().over(windowSpec))

# Step 4: Filter new records
df_new = df_source.filter(col("record_id") > last_recordid)

# ✅ Fix: Use rdd.isEmpty() instead of isEmpty()
if df_new.rdd.isEmpty():
    print("No new records to process. Exiting...")
    spark.stop()
    exit()

# Step 5: Transformations
print("Step 4: Performing transformations...")

# Remove rows where critical fields are NULL
df_transformed = df_new.filter(col("route").isNotNull())

# Add ingestion timestamp
df_transformed = df_transformed.withColumn("ingestion_timestamp", current_timestamp())

# Filter out unwanted values in timedetails
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

# Display transformed data for debugging
df_transformed.show()

# Step 6: Write transformed data to Hive table (append mode to avoid overwriting)
print("Step 5: Writing transformed data to Hive table...")
df_transformed.write.mode("append").format("hive").insertInto(target_table)

print("ETL Process Completed Successfully!")

# Stop Spark Session
spark.stop()
