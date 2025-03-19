#!/bin/bash

# Wait for Airflow to be ready
echo "Waiting for Airflow to be ready..."
sleep 30

# Add Spark connection
echo "Adding Spark connection..."
airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077' \
    --conn-extra '{"queue": "default"}'

# Add LakeFS connection
echo "Adding LakeFS connection..."
airflow connections add 'lakefs_conn' \
    --conn-type 'http' \
    --conn-host "${LAKEFS_ENDPOINT:-http://lakefs:8000}" \
    --conn-login "${LAKEFS_ACCESS_KEY:-AKIAJBWUDLDFGJY36X3Q}" \
    --conn-password "${LAKEFS_SECRET_KEY:-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}"

# Add ClickHouse connection
echo "Adding ClickHouse connection..."
airflow connections add 'clickhouse_conn' \
    --conn-type 'http' \
    --conn-host 'clickhouse' \
    --conn-port '8123' \
    --conn-login 'admin' \
    --conn-password 'admin' \
    --conn-schema 'ev_charging'

echo "Connections setup completed!" 