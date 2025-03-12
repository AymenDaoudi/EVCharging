#!/bin/bash

# Start the Spark master
/opt/bitnami/scripts/spark/run.sh &

# Wait for the master to be ready
sleep 10

# Submit the Spark streaming job
spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.network.timeout=${SPARK_NETWORK_TIMEOUT} \
    --conf spark.rpc.message.maxSize=${SPARK_RPC_MESSAGE_MAXSIZE} \
    --conf spark.driver.maxResultSize=${SPARK_DRIVER_MAXRESULTSIZE} \
    --conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY} \
    --conf spark.driver.memory=${SPARK_DRIVER_MEMORY} \
    --conf "spark.hadoop.fs.s3a.access.key=${LAKEFS_ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${LAKEFS_SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.endpoint=${LAKEFS_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --jars "/opt/bitnami/spark/jars/*" \
    /opt/bitnami/spark/scripts/spark_app.py

# Keep the container running
tail -f /dev/null 