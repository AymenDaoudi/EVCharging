#!/bin/bash

# Check the status of Kafka Connect
status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors)

if [ "$status" == 200 ]; then
    echo "Kafka broker is healthy"
    exit 0
else
    echo "Kafka broker is not healthy"
    exit 1
fi