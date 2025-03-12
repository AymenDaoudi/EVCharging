# Airflow with LakeFS Integration

This directory contains the Airflow DAGs and plugins for interacting with LakeFS.

## Setup

The Airflow services are configured in the main `docker-compose.yml` file and include:

1. `airflow-postgres` - PostgreSQL database for Airflow metadata
2. `airflow-webserver` - Airflow web UI
3. `airflow-scheduler` - Airflow scheduler
4. `airflow-init` - Initialization service for Airflow

## Accessing Airflow

Once the services are up and running, you can access the Airflow web UI at:

```
http://localhost:8090
```

Default credentials:
- Username: `admin`
- Password: `admin`

## LakeFS Connection

The connection to LakeFS is configured using an environment variable in the docker-compose.yml file:

```
AIRFLOW_CONN_LAKEFS_DEFAULT=http://AKIAJBWUDLDFGJY36X3Q:sYAuql0Go9qOOQlQNPEw5Cg2AOzLZebnKgMaVyF+@lakefs:8000
```

This connection is used in the example DAG to interact with LakeFS.

## Example DAG

The `lakefs_example_dag.py` demonstrates how to:

1. List repositories in LakeFS
2. Create a new repository if it doesn't exist
3. Create a new branch in the repository

## Adding Custom DAGs

To add your own DAGs, place them in the `dags` directory. They will be automatically picked up by Airflow.

## Dependencies

If you need additional Python packages for your DAGs, add them to the `plugins/requirements.txt` file. 