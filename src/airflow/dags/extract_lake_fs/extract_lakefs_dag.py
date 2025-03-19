import os, logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from extract_lake_fs.lakefs_sense_and_get_commit_operator import LakeFSSenseAndGetCommitOperator

REPOSITORY = os.getenv("LAKEFS_REPOSITORY", "charging-data")
BRANCH = os.getenv("LAKEFS_BRANCH", "main")
LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT", "http://lakefs:8000")
LAKEFS_ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY", "AKIAJBWUDLDFGJY36X3Q")
LAKEFS_SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY", "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")

default_args = {
    'owner': 'aymen_daoudi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
    'repo': REPOSITORY,
    'branch': BRANCH,
    'default-branch': BRANCH
}

# Downstream task: process the commit details pulled from XCom
def process_commit(**kwargs):
    # Get the task instance from kwargs
    ti = kwargs['ti']
    commit_details = ti.xcom_pull(task_ids='sense_commit')
    
    logging.info(f'there was {len(commit_details.items())}')
    logging.info(f"Print Commit details keys")
    for key in commit_details.keys():
        logging.info(f"KEY: {key}")
    
    logging.info(f"Print Commit details")
    
    if not commit_details:
        raise ValueError("No commit details found in XCom.")
    
    for key, value in commit_details.items():
        logging.info(f"FOR KEY: {key}, THERE IS VALUE: {value}")
    
    logging.info(f"Detected Commit ID: {commit_details.get('id')}")
    # Here, you could also trigger further processing, like a Spark job

with DAG(
    'lakefs_extract_dag',
    default_args=default_args,
    dag_display_name='LakeFS Extract Data',
    description='Detects merges to the main branch of the lakeFS repo, and extracts the merged data.',
    schedule_interval=timedelta(seconds=20),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['extract', 'lakefs'],
) as dag:
    
    # Wait for a commit on the branch
    sense_commit_task = LakeFSSenseAndGetCommitOperator(
        task_id='Sense_merges_to_LakeFS_main_branch',
        lakefs_conn_id='lakefs_conn',
        repo=default_args['repo'],
        branch=default_args['branch'],
        poke_interval=30,
        timeout=300
    )
    
    spark_transform_task = SparkSubmitOperator(
        task_id='Extract_data_from_lakefs',
        name='Extract data from lakeFS',
        conn_id='spark_default',
        application='/opt/airflow/spark/extract_lakefs_job.py',
        application_args=[
            '--commit_id', "{{ task_instance.xcom_pull(task_ids='Sense_merges_to_LakeFS_main_branch').get('id') }}",
            '--repository', default_args['repo'],
            '--branch', default_args['branch']
        ],
        conf={
            'spark.hadoop.fs.s3a.log.level': 'DEBUG',
            'spark.master': 'spark://spark-master:7077',
            'spark.driver.memory': '1g',
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1',
            'spark.driver.cores': '1',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.endpoint': LAKEFS_ENDPOINT,
            'spark.hadoop.fs.s3a.access.key': LAKEFS_ACCESS_KEY,
            'spark.hadoop.fs.s3a.secret.key': LAKEFS_SECRET_KEY,
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.hadoop.fs.s3a.change.detection.mode': 'none',
            'spark.hadoop.fs.s3a.committer.magic.enabled': 'true',
            'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4',
            'spark.jars': '/opt/airflow/jars/clickhouse-jdbc-0.6.5-all.jar'
        },
        verbose=True
    )

    # Define the task dependencies
    sense_commit_task >> spark_transform_task