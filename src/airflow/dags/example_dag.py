from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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
    'ev_charging_example',
    default_args=default_args,
    description='A simple example DAG for EV charging data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define a Python function for a task
def print_hello():
    return 'Hello from EV Charging Data Pipeline!'

# Task 1: Print hello
task_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Task 2: Print date
task_date = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Task 3: Echo a message
task_echo = BashOperator(
    task_id='echo_message',
    bash_command='echo "Processing EV charging data..."',
    dag=dag,
)

# Set task dependencies
task_hello >> task_date >> task_echo 