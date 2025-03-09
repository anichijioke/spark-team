from pyspark.sql import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local").appName("team_pyspark").enableHiveSupport().getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.170.23.150:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "tfl_underground_pyspark").option("user", "consultants").option("password", "WelcomeItc@2022").load()
df.printSchema()

# Step 2: Transform - Clean and Format the Data
    # Convert 'Timestamp' to proper timestamp format
df_transformed = df.withColumn("Timestamp", F.to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))

    # Replace "N/A" with null
df_transformed = df.replace("N/A", None)
    
    # Step 3: Insert Transformed Data into Hive Table
df_transformed.write.mode("append").insertInto("tfl_underground")


df.write.mode("overwrite").saveAsTable("big_data_jan2025.full-load-pyspark")
print("Successfully Load to Hive")
