from datetime import datetime, timedelta
from pprint import pprint
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from lakefs_provider.sensors.commit_sensor import LakeFSCommitSensor
from lakefs_provider.sensors.file_sensor import LakeFSFileSensor
from airflow.models.taskinstance import TaskInstance
import logging
import os

REPOSITORY = os.getenv("LAKEFS_REPOSITORY", "charging-data")
BRANCH = os.getenv("LAKEFS_BRANCH", "main")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'repo': REPOSITORY,
    'branch': BRANCH,
    'default-branch': BRANCH
}

@task(task_id="print_commit_details")
def print_commit_details(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    print('Commit retrieved **************************')
    logging.info('Commit retrieved **************************')
    
    logging.info('printing kwargs:')
    logging.info(f'kwargs have {len(kwargs)} items')
    
    for key, value in kwargs.items():
        logging.info(f"FOR KEY: {key}, THERE IS VALUE: {value}")
    
    logging.info('Finished enumerating kwargs ****************************************************')
    
    ti = kwargs['ti']
    commit_details = ti.xcom_pull(task_ids='sense_commit')
    
    logging.info('Enumerating commit details **************************')
    
    for key, value in commit_details.items():
        logging.info(f"FOR KEY: {key}, THERE IS VALUE: {value}")
    
    logging.info('****************************************************')
    
    
    return "Whatever you return gets printed in the logs"

with DAG(
    'extract_from_lakefs',
    default_args=default_args,
    description='Detects a merge to the main branch and extracts the data from the lakefs repo',
    schedule_interval=timedelta(seconds=20),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['extract', 'lakefs'],
) as dag:
    
    # Wait for a commit on the branch
    sense_commit_task = LakeFSCommitSensor(
        task_id='sense_commit',
        lakefs_conn_id='lakefs_conn',
        repo=default_args['repo'],
        branch=default_args['branch'],
        poke_interval=30,
        timeout=300
    )

    print_commit_task = print_commit_details()
    
    sense_commit_task >> print_commit_task