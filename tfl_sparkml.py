# =======================
# IMPORT LIBRARIES
# =======================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, month, year, regexp_replace
)
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

# =======================
# CREATE SPARK SESSION WITH HIVE SUPPORT
# =======================
spark = SparkSession.builder \
    .appName("Hive_Spark_Classification") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.num.executors", "4") \
    .enableHiveSupport() \
    .getOrCreate()

# =======================
# READ DATA FROM HIVE TABLE
# =======================
df = spark.sql("SELECT * FROM default.tfl_underground_result_n")
df = df.repartition(50)

# =======================
# PRINT SCHEMA TO DEBUG COLUMN NAMES
# =======================
df.printSchema()

# =======================
# FEATURE ENGINEERING
# =======================

df = df.withColumn('hour', hour(col('ingestion_timestamp')))
df = df.withColumn('day_of_week', dayofweek(col('ingestion_timestamp')))
df = df.withColumn('month', month(col('ingestion_timestamp')))
df = df.withColumn('year', year(col('ingestion_timestamp')))

# Remove special characters from 'reason' column
df = df.withColumn('reason', regexp_replace(col('reason'), '[^a-zA-Z0-9 ]', ''))

# =======================
# STRING INDEXING (Convert categorical to numeric)
# =======================
indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_index", handleInvalid="keep").fit(df)
    for col in ["line", "reason", "route", "status"]
]

for indexer in indexers:
    df = indexer.transform(df)

# =======================
# ONE-HOT ENCODING (Using OneHotEncoderEstimator for Cloudera compatibility)
# =======================
encoder = OneHotEncoderEstimator(
    inputCols=["line_index", "reason_index", "route_index"],
    outputCols=["line_vec", "reason_vec", "route_vec"]
)

df = encoder.fit(df).transform(df)

# =======================
# VECTOR ASSEMBLER (Combine all features)
# =======================
assembler = VectorAssembler(
    inputCols=["hour", "day_of_week", "month", "year", "line_vec", "reason_vec", "route_vec"],
    outputCol="features"
)
data = assembler.transform(df).select("features", "status_index")

# =======================
# TRAIN-TEST SPLIT
# =======================
train_data, test_data = data.randomSplit([0.8, 0.2], seed=123)

# =======================
# CACHE TRAIN DATA
# =======================
train_data.cache()
train_data.count()  # Preload train data

# =======================
# MODEL TRAINING AND PREDICTION
# =======================

# Train Random Forest Model with optimized settings
print("Training Random Forest...")
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="status_index",
    numTrees=50,
    maxDepth=10
)
rf_model = rf.fit(train_data)

# Predict on Test Data
rf_preds = rf_model.transform(test_data)

# =======================
# SELECT COLUMNS TO MATCH EXPECTED OUTPUT
# =======================
output_df = rf_preds.select("features", "status_index", "rawPrediction", "probability", "prediction")

# Show sample of output
output_df.show(5, truncate=False)

# =======================
# SAVE OUTPUT TO CSV (For Jenkins)
# =======================
output_path = "output/predictions.csv"

output_df.write.csv(output_path, header=True, mode="overwrite")

print("Predictions saved at {}".format(output_path))  # Avoid f-string for compatibility

# =======================
# STOP SPARK SESSION
# =======================
spark.stop()
