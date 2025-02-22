#!/bin/bash

# Start the Spark master
/opt/bitnami/scripts/spark/run.sh &

# Wait for the master to be ready
sleep 10

# Submit the Spark streaming job
spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.commons:commons-pool2:2.11.1 \
    --conf spark.network.timeout=800s \
    --conf spark.rpc.message.maxSize=1024 \
    --conf spark.driver.maxResultSize=2g \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    --jars "/opt/bitnami/spark/jars/*" \
    /opt/bitnami/spark/scripts/spark_kafka_streaming.py

# Keep the container running
tail -f /dev/null 