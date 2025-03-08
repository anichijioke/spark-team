from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

max_vehicle_id = spark.sql("SELECT max(vehicle_id) FROM bigdata_nov_2024.stolen_vehicles")
m_vehicle_id = max_vehicle_id.collect()[0][0]

query = 'SELECT * FROM stolen_cars WHERE vehicle_id > ' + str(m_vehicle_id)

more_data = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "mailto:welcomeitc@2022").option("query", query).load()

more_data.write.mode("append").saveAsTable("bigdata_nov_2024.stolen_vehicles")
print("Successfully Load to Hive")