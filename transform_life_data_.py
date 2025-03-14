from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, expr

# Initialize Spark session
spark = SparkSession.builder.appName("TfL Tube Status Transformation").getOrCreate()

# Load Data (Assuming DataFrame df_tube_status)
df = spark.read.format("csv").option("header", "true").load("your_file.csv")  # Replace with actual data source

# Rename columns to avoid dots (.)
df = df.withColumnRenamed("tfl_tube_status.name", "line_name") \
       .withColumnRenamed("tfl_tube_status.linestatus", "line_status") \
       .withColumnRenamed("tfl_tube_status.timedetails", "time_details")

# Clean the "line_status" column
df = df.withColumn("line_status", regexp_replace(col("line_status"), r'^\["\[\\\\"|"\\\]\"]$', ''))  # Remove extra characters
df = df.withColumn("line_status", regexp_replace(col("line_status"), r'\\', ''))  # Remove escape characters
df = df.withColumn("line_status", regexp_replace(col("line_status"), r'["\[\]]', ''))  # Remove brackets and quotes

# Show transformed data
import ace_tools as tools
tools.display_dataframe_to_user(name="Transformed TfL Tube Status", dataframe=df)
