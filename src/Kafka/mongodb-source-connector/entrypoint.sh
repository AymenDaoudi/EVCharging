#!/bin/bash

echo "Kafka Connect is ready."

echo "Creating MongoDB Source Connector ..."

curl -X POST -H "Content-Type: application/json" -d @- http://kafka-connect:8083/connectors <<EOF
{
  "name": "ivado_db_source",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "connection.uri": "mongodb://User:Pass@mongo-db-node-1:30001,mongo-db-node-2:30002,mongo-db-node-3:30003/Db?replicaSet=replica-set",
    "database": "Db",
    "collection": "ChargingEvents",
    "pipeline": "[{'\$match': {'operationType': {'\$in': ['insert', 'update', 'replace'], }}},{'\$project': {'_id': 1,'fullDocument': 1,'ns': 1,}}]",
    "publish.full.document.only": "true",
    "topic.namespace.map": "{\"*\":\"Db.ChargingEvents\"}",
    "copy.existing": "true"
  }
}
EOF

echo "Finished installing Mongo Kafka source connector!"

# Keep this container alive and running
tail -f /dev/null