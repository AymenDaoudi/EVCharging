#!/bin/bash

# Execute the mongosh command to get the replica set status and capture the output
STATUS=$(mongosh 'mongodb://admin:admin@mongo-db-node-1:30001' --eval 'rs.status().ok')

# Check if the output equals 1
if [ "$STATUS" -eq 1 ]; then
    echo "ReplicaSet is OK"
    exit 0
else
    echo "ReplicaSet is NOT OK"
    exit 1
fi