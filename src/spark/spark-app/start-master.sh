#!/bin/bash

# Start the Spark master
/opt/bitnami/scripts/spark/run.sh &

# Wait for the master to be ready
sleep 10

# Submit the Spark streaming job
spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.commons:commons-pool2:2.11.1,org.apache.hadoop:hadoop-aws:3.3.1 \
    --conf spark.network.timeout=60s \
    --conf spark.rpc.message.maxSize=1024 \
    --conf spark.driver.maxResultSize=2g \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --jars "/opt/bitnami/spark/jars/*" \
    /opt/bitnami/spark/scripts/spark_app.py \
    /opt/bitnami/spark/scripts/lakefs_manager.py \
    /opt/bitnami/spark/scripts/charging_events_stream_processor.py

# Keep the container running
tail -f /dev/null 