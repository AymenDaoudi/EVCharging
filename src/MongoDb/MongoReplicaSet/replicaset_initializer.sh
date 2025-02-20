#!/bin/bash

# Connect to first mongodb instance's and initiate the replica set
echo 'Initializing the replica set...'
mongosh 'mongodb://admin:admin@mongo-db-node-1:30001' --eval '
    config = {
      "_id" : "replica-set",
      "members" : [
        {
          "_id" : 0,
          "host" : "host.docker.internal:37001"
        },
        {
          "_id" : 1,
          "host" : "host.docker.internal:37002"
        },
        {
          "_id" : 2,
          "host" : "host.docker.internal:37003"
        }
      ]
    };
    rs.initiate(config);
'
echo 'Finished configuring and initiating replica set...'
echo 'Waiting 15 SECONDS to complete election...'
sleep 15

# Execute the mongosh command to get the replica set status and capture the output
STATUS=$(mongosh 'mongodb://admin:admin@mongo-db-node-1:30001' --eval 'rs.status().ok')

# Check if the output equals 1
if [ "$STATUS" -eq 1 ]; then
    echo "ReplicaSet is OK"
    # Keep this container alive and running
    tail -f /dev/null
else
    echo "ReplicaSet is NOT OK"
    exit 1
fi