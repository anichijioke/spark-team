# =======================
# IMPORT LIBRARIES
# =======================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, year, regexp_replace
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier

# =======================
# CREATE SPARK SESSION WITH HIVE SUPPORT
# =======================
spark = SparkSession.builder \
    .appName("Hive_Spark_Classification") \
    .enableHiveSupport() \
    .getOrCreate()

# =======================
# READ DATA FROM HIVE TABLE
# =======================
df = spark.sql("SELECT * FROM scala_tfl_underground")

# =======================
# FEATURE ENGINEERING
# =======================
df = df.withColumn('hour', hour(col('timestamp')))
df = df.withColumn('day_of_week', dayofweek(col('timestamp')))
df = df.withColumn('month', month(col('timestamp')))
df = df.withColumn('year', year(col('timestamp')))

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
# ONE-HOT ENCODING
# =======================
encoder = OneHotEncoder(
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
# MODEL TRAINING AND PREDICTION
# =======================

# Train Random Forest Model
print("Training Random Forest...")
rf = RandomForestClassifier(featuresCol="features", labelCol="status_index", numTrees=50)
rf_model = rf.fit(train_data)

# Predict on Test Data
rf_preds = rf_model.transform(test_data)

# =======================
# SAVE OUTPUT TO CSV (For Jenkins)
# =======================
output_path = "output/predictions.csv"

rf_preds.select("features", "status_index", "prediction") \
    .write.csv(output_path, header=True, mode="overwrite")

print("Predictions saved at {}".format(output_path))

# =======================
# STOP SPARK SESSION
# =======================
spark.stop()
