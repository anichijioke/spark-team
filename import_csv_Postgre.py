import psycopg2
import pandas as pd

# Step 1: Connect to PostgreSQL
conn = psycopg2.connect(
    host="18.170.23.150",
    database="testdb",
    user="consultants",
    password="WelcomeItc@2022"
)
cursor = conn.cursor()

# Step 3: Load CSV file
csv_file_path = r"C:\Users\anich\Downloads\query-hive-13823.csv"
data = pd.read_csv(csv_file_path)

# Rename columns to match database table
data.rename(columns={
    "tfl_underground.timestamp": "timestamp",
    "tfl_underground.line": "line",
    "tfl_underground.status": "status",
    "tfl_underground.reason": "reason",
    "tfl_underground.delay_time": "delay_time",
    "tfl_underground.route": "route"
}, inplace=True)

# Step 4: Insert data into the table
for _, row in data.iterrows():
    cursor.execute("""
        INSERT INTO tfl_underground_pyspark (timestamp, line, status, reason, delay_time, route)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (row["timestamp"], row["line"], row["status"], row["reason"], row["delay_time"], row["route"]))

conn.commit()

# Step 5: Close the connection
cursor.close()
conn.close()
print(data.head())  # Shows the first 5 rows
#print("Data insertion complete and connection closed.")
