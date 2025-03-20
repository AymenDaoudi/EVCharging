import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'aymen_daoudi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'extract_vehicles_from_mongodb',
    default_args=default_args,
    description='Extract vehicle data from MongoDB using Spark',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule_interval='@once',
    is_paused_upon_creation=False,
    tags=['extract', 'mongodb', 'electric_vehicles'],
) as dag:
    
    spark_extract_task = SparkSubmitOperator(
        task_id='extract_vehicles_from_mongodb',
        name='Extract vehicles data from MongoDB',
        conn_id='spark_default',
        application='/opt/airflow/spark/etl_vehicles_job.py',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.driver.memory': '1g',
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1',
            'spark.driver.cores': '1',
            'spark.jars.packages': 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.2.1'
        },
        verbose=True
    )

    # Define the task dependencies (in this case, we only have one task)
    spark_extract_task
