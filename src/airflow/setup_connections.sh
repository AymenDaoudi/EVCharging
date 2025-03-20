#!/bin/bash

# Wait for Airflow to be ready
echo "Waiting for Airflow to be ready..."
sleep 30

# Add Spark connection
echo "Adding Spark connection..."
airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host "${SPARK_HOST:-spark://spark-master}" \
    --conn-port "${SPARK_PORT:-7077}" \
    --conn-extra "{\"queue\": \"${SPARK_QUEUE:-default}\"}"

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
    --conn-host "${CLICKHOUSE_HOST:-clickhouse}" \
    --conn-port "${CLICKHOUSE_PORT:-8123}" \
    --conn-login "${CLICKHOUSE_USER:-admin}" \
    --conn-password "${CLICKHOUSE_PASSWORD:-admin}" \
    --conn-schema "${CLICKHOUSE_DATABASE:-ev_charging}"

# Add MongoDB connection
echo "Adding MongoDB connection..."
airflow connections add 'mongodb_conn' \
    --conn-type 'mongo' \
    --conn-host "${MONGO_HOST:-mongo-db}" \
    --conn-port "${MONGO_PORT:-27017}" \
    --conn-login "${MONGO_USER:-User}" \
    --conn-password "${MONGO_PASSWORD:-Pass}" \
    --conn-extra "{\"database\": \"${MONGO_DATABASE:-Db}\", \"electric_vehicles_collection\": \"${MONGO_ELECTRIC_VEHICLES_COLLECTION:-ElectricVehicles}\", \"charging_stations_collection\": \"${MONGO_CHARGING_STATIONS_COLLECTION:-ChargingStations}\"}"

echo "Connections setup completed!" 