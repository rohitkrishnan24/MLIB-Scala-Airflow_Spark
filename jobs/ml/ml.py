from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Initialize Spark session
spark = SparkSession.builder.appName("MLExample").getOrCreate()

# Your CSV file path
csv_file_path = "/Users/shivramsriramulu/Downloads/tripdata.csv"

# Read CSV into a PySpark DataFrame
df = spark.read.option("header", "true").csv(csv_file_path)

# Select relevant columns for the linear regression model
selected_columns = ["start_lat", "start_lng", "end_lat", "end_lng"]
df = df.select(selected_columns)

# Drop any rows with missing values
df = df.na.drop()

# Create a feature vector by combining input features
assembler = VectorAssembler(inputCols=selected_columns, outputCol="features")
df = assembler.transform(df)

# Split the data into training and testing sets
train_data, test_data = df.randomSplit([0.8, 0.2], seed=123)

# Create a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="end_lng")

# Fit the model to the training data
model = lr.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Show the predicted values
predictions.select("features", "prediction").show()

# Stop the Spark session
spark.stop()
