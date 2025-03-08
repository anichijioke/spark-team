from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import pyspark.sql.functions as F

# Initialize SparkSession with Hive support
spark = SparkSession.builder \
    .appName("Postgres to Hive") \
    .config("spark.jars", r"C:\Users\anich\OneDrive\Desktop\ITC-class\Team_Spark\postgresql-42.7.5.jar") \
    .enableHiveSupport() \
    .getOrCreate()

#.config("spark.sql.warehouse.dir", "hdfs://your-hive-metastore-path") \
#.config("spark.sql.hive.metastore.version", "2.3.0") \
    
# Specify the Hive database (if needed)
#spark.sql("USE tfl_data_db") 

# PostgreSQL connection properties
url = "jdbc:postgresql://18.170.23.150/testdb?ssl=false"  # Replace with your PostgreSQL connection details
properties = {
    "user": "consultants",        # PostgreSQL username
    "password": "WelcomeItc@2022",    # PostgreSQL password
    "driver": "org.postgresql.Driver"
}

# Step 1: Read data from PostgreSQL into a Spark DataFrame
try:
    df = spark.read.format("jdbc").options(
        url=url,
        dbtable="tfl_underground_pyspark",  # Replace with your PostgreSQL table name
        user=properties["user"],
        password=properties["password"],
        driver=properties["driver"]
    ).load()
    
    print("Data successfully read from PostgreSQL:")
    df.show(50)

    # Step 2: Transform - Clean and Format the Data
    # Convert 'Timestamp' to proper timestamp format
    df_transformed = df.withColumn("Timestamp", F.to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))

    # Replace "N/A" with null
    df_transformed = df.replace("N/A", None)
    
    # Step 3: Insert Transformed Data into Hive Table
    df_transformed.write.mode("append").insertInto("tfl_underground")

    # Step 2: Write the DataFrame to Hive
    #df.write.mode("overwrite").saveAsTable("your_hive_table")  # Replace "your_hive_table" with the desired Hive table name

    print("Data successfully pushed to Hive table.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Stop the SparkSession
    spark.stop()
