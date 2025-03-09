from pyspark.sql import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

max_uid = spark.sql("SELECT max(uid) FROM big_data_jan2025.tfl_underground_pyspark")
max_uid = max_uid.collect()[0][0]

query = 'SELECT * FROM new_tfl2 WHERE uid > ' + str(max_uid)

more_data = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.170.23.150:5432/testdb").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").option("query", query).load()

# Step 2: Transform - Clean and Format the Data
    # Convert 'Timestamp' to proper timestamp format
df_transformed = more_data.withColumn("timestamp", F.to_timestamp(col("timestamp"), "dd/MM/yyyy HH:mm"))

    # Replace "N/A" with null
df_transformed = df_transformed.replace("N/A", None)

df_transformed.write.mode("append").saveAsTable("big_data_jan2025.tfl_underground_pyspark")
print("Successfully Load to Hive")
