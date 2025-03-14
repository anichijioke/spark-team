# -*- coding: utf-8 -*-
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp, date_format

# ✅ Initialize Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Start Spark Session
spark = SparkSession.builder.appName("Transform_Life_Data").enableHiveSupport().getOrCreate()

# ✅ Define Hive Database & Tables
HIVE_DB = "default"
SOURCE_TABLE = "tfl_tube_status"
TARGET_TABLE = "TFL_Tube_Result"

# ✅ Log the start of the process
logger.info("Loading data from source table: %s.%s", HIVE_DB, SOURCE_TABLE)

# ✅ Load Data from Hive
df = spark.sql("SELECT * FROM {}.{}".format(HIVE_DB, SOURCE_TABLE))

# ✅ Clean 'linestatus' Column
df = df.withColumn("linestatus", col("linestatus").getItem(0))  # Extract first element from array
df = df.withColumn("linestatus", regexp_replace(col("linestatus"), r'[\[\]\"]', ''))  # Remove brackets & quotes
df = df.withColumn("linestatus", regexp_replace(col("linestatus"), r'\\', ''))  # Remove backslashes

# ✅ Format 'timedetails' Column to "dd/MM/yyyy HH:mm:ss"
df = df.withColumn("timedetails", regexp_replace(col("timedetails"), r'\.\d+Z$', ''))  # Remove milliseconds & 'Z'
df = df.withColumn("timedetails", to_timestamp(col("timedetails"), "yyyy-MM-dd'T'HH:mm:ss"))  # Convert to timestamp
df = df.withColumn("timedetails", date_format(col("timedetails"), "dd/MM/yyyy HH:mm:ss"))  # Format to dd/MM/yyyy HH:mm:ss

# ✅ Remove NULL values in 'timedetails'
df = df.filter(col("timedetails").isNotNull())

# ✅ Log Data Processing Completion
logger.info("Data transformation completed successfully")

# ✅ Write Transformed Data Back to Hive
df.write.mode("overwrite").saveAsTable("{}.{}".format(HIVE_DB, TARGET_TABLE))

logger.info("Transformed data saved to %s.%s", HIVE_DB, TARGET_TABLE)

# ✅ Stop Spark Session
spark.stop()
logger.info("Spark session stopped successfully")
