import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def ml_job(**kwargs):
    from pyspark.sql import SparkSession
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import LinearRegression
    from datetime import datetime

    # Initialize Spark session
    spark = SparkSession.builder.appName("SimpleMLExample").getOrCreate()

    # Generate synthetic data
    data = [(1.0, 2.0, 3.0, 4.0),
            (2.0, 3.0, 4.0, 5.0),
            (3.0, 4.0, 5.0, 6.0)]

    columns = ["feature1", "feature2", "feature3", "label"]

    # Create a PySpark DataFrame
    df = spark.createDataFrame(data, columns)

    # Create a feature vector by combining input features
    assembler = VectorAssembler(inputCols=columns[:-1], outputCol="features")
    df = assembler.transform(df)

    # Split the data into training and testing sets
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=123)

    # Create a Linear Regression model
    lr = LinearRegression(featuresCol="features", labelCol=columns[-1])

    # Fit the model to the training data
    model = lr.fit(train_data)

    # Make predictions on the test data
    predictions = model.transform(test_data)

    # Log the predicted values
    predictions.select("features", "prediction").show()

    # Log completion message
    log_message = f"ML Job completed successfully at {datetime.now()}"
    kwargs['ti'].xcom_push(key='ml_job_logs', value=log_message)
    print(log_message)

    # Stop the Spark session
    spark.stop()

    
dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "owner": "Shivram",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python/etl.py",
    dag=dag
)
ml_python_job = PythonOperator(
    task_id="ml_python_job",
    python_callable=ml_job,  # Corrected to reference ml_job function
    dag=dag
)
scala_job = SparkSubmitOperator(
    task_id="scala_job",
    conn_id="spark-conn",
    application="jobs/scala/target/scala-2.12/word-count_2.12-0.1.jar",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> python_job >> ml_python_job >> scala_job >> end