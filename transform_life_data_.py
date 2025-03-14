# -*- coding: utf-8 -*-
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

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
df = df.withColumn("linestatus", col("linestatus").getItem(0))  # Extract first element
df = df.withColumn("linestatus", regexp_replace(col("linestatus"), r'\\', ''))

# ✅ Log Data Processing Completion
logger.info("Data transformation completed successfully")

# ✅ Write Transformed Data Back to Hive
df.write.mode("overwrite").saveAsTable("{}.{}".format(HIVE_DB, TARGET_TABLE))

logger.info("Transformed data saved to %s.%s", HIVE_DB, TARGET_TABLE)

# ✅ Stop Spark Session
spark.stop()
logger.info("Spark session stopped successfully")
