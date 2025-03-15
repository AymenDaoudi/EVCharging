from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_example_job',
    default_args=default_args,
    description='A DAG to submit an example Spark job',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Task 1: Print a message before submitting the Spark job
start_task = BashOperator(
    task_id='start_task',
    bash_command='echo "Starting Spark job submission..."',
    dag=dag,
)

# Task 2: Submit the Spark job
# Note: We're using the SparkSubmitOperator to submit the job to the Spark master
submit_spark_job = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='/opt/airflow/spark/example_spark_job.py', 
    conn_id='spark_default',  # Connection ID for Spark (will be configured in Airflow)
    name='example_spark_job',  # Name of the Spark job
    verbose=True,
    # Spark connection details
    # These match the configuration in the docker-compose.yml file
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
        'spark.executor.cores': '1',
        'spark.driver.cores': '1',
    },
    dag=dag,
)

# Task 3: Print a message after the Spark job completes
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "Spark job submission completed!"',
    dag=dag,
)

# Define task dependencies
start_task >> submit_spark_job >> end_task 