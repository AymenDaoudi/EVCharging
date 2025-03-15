# Airflow Setup for EV Charging Data Pipeline

This directory contains the Airflow setup for the EV Charging Data Pipeline project.

## Components

- **Airflow Webserver**: Web UI for managing and monitoring DAGs
- **Airflow Scheduler**: Responsible for scheduling and triggering tasks
- **Postgres Database**: Metadata database for Airflow

## Docker Setup

The Airflow services are built using a custom Dockerfile that:
1. Uses the official Apache Airflow image as a base
2. Installs system dependencies
3. Installs Python packages from requirements.txt

## Usage

1. Start the Airflow services using Docker Compose:
   ```
   docker-compose up -d airflow-postgres airflow-webserver airflow-scheduler
   ```

2. Access the Airflow UI:
   - URL: http://localhost:8090
   - Default username: admin
   - Default password: admin

3. The example DAG (`ev_charging_example`) should be visible in the UI.

## Python Dependencies

The `requirements.txt` file contains Python packages that will be installed in the Airflow image during build. If you need additional packages for your DAGs, add them to this file and rebuild the image.

Current dependencies include:
- pandas and numpy for data manipulation
- pyspark for Spark integration
- pymongo for MongoDB integration
- python-dotenv for environment variables
- requests for HTTP requests
- Airflow provider packages for Spark, MongoDB, and HTTP
- s3fs and boto3 for S3/MinIO integration

## Adding New DAGs

Place new DAG files in the `dags` directory. They will be automatically picked up by Airflow.

## Example DAG

The example DAG (`example_dag.py`) demonstrates a simple workflow with three tasks:
1. A Python task that prints a hello message
2. A Bash task that prints the current date
3. A Bash task that echoes a message about processing EV charging data

The tasks run in sequence: task_hello → task_date → task_echo 