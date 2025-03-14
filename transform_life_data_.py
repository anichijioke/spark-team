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
SOURCE_TABLE = "tfl_undergroundrecord"
TARGET_TABLE = "tfl_underground_result_n"

# ✅ Log the start of the process
logger.info("Loading data from source table: %s.%s", HIVE_DB, SOURCE_TABLE)

# ✅ Load Data from Hive
df = spark.sql(f"SELECT * FROM {HIVE_DB}.{SOURCE_TABLE}")

# ✅ Clean 'linestatus' Column
df = df.withColumn("linestatus", regexp_replace(col("linestatus"), r'\\', ''))  # Remove escape characters
df = df.withColumn("linestatus", regexp_replace(col("linestatus"), r'["\[\]]', ''))  # Remove brackets and quotes

# ✅ Log Data Processing Completion
logger.info("Data transformation completed successfully")

# ✅ Write Transformed Data Back to Hive
df.write.mode("overwrite").saveAsTable(f"{HIVE_DB}.{TARGET_TABLE}")

logger.info("Transformed data saved to %s.%s", HIVE_DB, TARGET_TABLE)

# ✅ Stop Spark Session
spark.stop()
logger.info("Spark session stopped successfully")
