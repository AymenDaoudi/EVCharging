#!/bin/bash

# Generate a random 1024-byte key and save it to mongo-keyfile
echo "Creating directory for keyfile"
mkdir -p /etc/mongodb/keys/

echo "Generating keyfile"
openssl rand -base64 756 > /etc/mongodb/keys/mongo-key

# Set the keyfile permissions to 400 and change the owner
echo "Setting permissions and owner"
chmod 400 /etc/mongodb/keys/mongo-key
chown mongodb:mongodb /etc/mongodb/keys/mongo-key