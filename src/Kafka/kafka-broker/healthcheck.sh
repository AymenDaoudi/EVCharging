#!/bin/bash

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic healthcheck
echo "System is healthy" | kafka-console-producer --broker-list localhost:9092 --topic healthcheck
consumed=$(kafka-console-consumer --bootstrap-server localhost:9092 --topic healthcheck --from-beginning --max-messages 1)
if [ "$consumed" == "System is healthy" ]; then
    echo "Kafka broker is healthy"
    exit 0
else
    echo "Kafka broker is not healthy"
    exit 1
fi