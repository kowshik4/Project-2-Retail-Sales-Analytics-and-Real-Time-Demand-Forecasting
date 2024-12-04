import sqlite3
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Retail Sales Streaming") \
    .getOrCreate()

# Function to store data in SQLite
def store_in_sqlite(data):
    # Connect to SQLite database (it creates the file if it doesn't exist)
    conn = sqlite3.connect("streaming_data.db")
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS messages
                      (message TEXT)''')

    cursor.execute("INSERT INTO messages (message) VALUES (?)", (data,))
    conn.commit()
    conn.close()

streaming_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Process the data (cast value to string)
processed_df = streaming_df.selectExpr("cast(value as string) as message")

# Write output to SQLite database
def write_to_sqlite(batch_df, batch_id):
    # Collect the batch data
    messages = batch_df.collect()
    for message in messages:
        store_in_sqlite(message["message"])

# Start streaming query
query = processed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_sqlite) \
    .start()

# Await termination to keep the stream running
query.awaitTermination()
