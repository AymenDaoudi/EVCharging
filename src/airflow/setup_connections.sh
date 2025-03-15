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

echo "Connections setup completed!" 