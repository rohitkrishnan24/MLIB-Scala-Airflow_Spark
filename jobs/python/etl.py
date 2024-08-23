import json
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
import logging

# Function to read data from a CSV file
def read_csv(file_path):
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        return df
    except Exception as e:
        logging.error(f"Error reading data from CSV file: {e}")
        return None

# Function to calculate ride duration in minutes
def calculate_duration(started_at, ended_at):
    return (unix_timestamp(ended_at) - unix_timestamp(started_at)) / 60

# JDBC URL and properties for MySQL connection
mysql_properties = {
    "user": "root",
    "password": "Shivramsriramulu1234",
    "driver": "com.mysql.jdbc.Driver"
}

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "/Users/shivramsriramulu/Downloads/mysql-connector-java-8.0.27.jar") \
    .getOrCreate()

# CSV file path
csv_file_path = "/Users/shivramsriramulu/Downloads/tripdata.csv"  # Correct path

# Read data from the CSV file
df = read_csv(csv_file_path)

if df is not None:
    # Show DataFrame content before transformation
    print("DataFrame before transformation:")
    df.show()

    # Add a transformation to calculate ride duration
    df_transformed = df.withColumn("duration_minutes", calculate_duration(col("started_at"), col("ended_at")))

    # Show DataFrame content after transformation
    print("DataFrame after transformation:")
    df_transformed.show()

    # Write the transformed DataFrame to MySQL
    df_transformed.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/sau", table="tripdata_transformed", mode="overwrite", properties=mysql_properties)
else:
    logging.error("Error reading CSV data. Exiting.")

# Stop the Spark session
spark.stop()