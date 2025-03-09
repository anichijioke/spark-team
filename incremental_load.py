from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

max_uid = spark.sql("SELECT max(uid) FROM big_data_jan2025.tfl_underground_pysparks")
max_uid = max_uid.collect()[0][0]

query = 'SELECT * FROM new_tfl2 WHERE uid > ' + str(max_uid)

more_data = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.170.23.150:5432/testdb").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").option("query", query).load()



more_data.write.mode("append").saveAsTable("bigdata_nov_2024.stolen_vehicles")
print("Successfully Load to Hive")
