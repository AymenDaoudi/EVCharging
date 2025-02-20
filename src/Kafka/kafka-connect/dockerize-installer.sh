#!/bin/bash

apt-get update
apt-get install -y wget
rm -rf /var/lib/apt/lists/*

wget https://github.com/jwilder/dockerize/releases/download/v0.6.1/dockerize-linux-amd64-v0.6.1.tar.gz

#tar -C /usr/local/bin -xzvf ./usr/local/bin/dockerize/dockerize-linux-amd64-v0.6.1.tar.gz

tar -xzvf dockerize-linux-amd64-v0.6.1.tar.gz

chmod +x dockerize

rm dockerize-linux-amd64-v0.6.1.tar.gz