#!/bin/bash

# Start the Spark master
/opt/bitnami/scripts/spark/run.sh &

# Wait for the master to be ready
sleep 10

# Submit the Spark streaming job
spark-submit ${SPARK_HOME}/scripts/spark_app.py

# Keep the container running
tail -f /dev/null 