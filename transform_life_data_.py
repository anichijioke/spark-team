from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, expr

# Initialize Spark session
spark = SparkSession.builder.appName("TfL Tube Status Transformation").getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = "tfl_tube_status"

logger.info("Loading data from source table: %s.%s", HIVE_DB, SOURCE_TABLE)

# Load data from the source table
df_source = spark.sql("SELECT * FROM {}.{}".format(HIVE_DB, SOURCE_TABLE))


# Rename columns to avoid dots (.)
df = df_source.withColumnRenamed("tfl_tube_status.name", "line_name") \
       .withColumnRenamed("tfl_tube_status.linestatus", "line_status") \
       .withColumnRenamed("tfl_tube_status.timedetails", "time_details")

# Clean the "line_status" column
df = df.withColumn("line_status", regexp_replace(col("line_status"), r'^\["\[\\\\"|"\\\]\"]$', ''))  # Remove extra characters
df = df.withColumn("line_status", regexp_replace(col("line_status"), r'\\', ''))  # Remove escape characters
df = df.withColumn("line_status", regexp_replace(col("line_status"), r'["\[\]]', ''))  # Remove brackets and quotes

# Show transformed data
import ace_tools as tools
tools.display_dataframe_to_user(name="Transformed TfL Tube Status", dataframe=df)
