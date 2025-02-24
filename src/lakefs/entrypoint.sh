#!/bin/sh

echo "Starting LakeFS..."
lakefs run &  # Start LakeFS in the background

echo "Started LakeFS"
echo "Waiting for LakeFS to be ready..."

until curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health | grep 200; do
  sleep 2
done

# Wait for setup endpoint to be available
sleep 20

echo "LakeFS is up! Creating lakectl config..."

# Create config file for lakectl
mkdir -p /home/lakefs
echo "credentials:
  access_key_id: ${LAKECTL_CREDENTIALS_ACCESS_KEY_ID}
  secret_access_key: ${LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY}
server:
  endpoint_url: http://localhost:8000" > /home/lakefs/.lakectl.yaml

# Wait for MinIO to be ready
echo "Waiting for MinIO..."
until curl -s -o /dev/null -w "%{http_code}" http://minio:9000/minio/health/live | grep 200; do
  sleep 2
done

echo "Creating repository using lakectl..."
lakectl repo create lakefs://charging-data s3://raw-data/charging-events --default-branch main

echo "Listing repositories..."
lakectl repo list

echo "Setup complete!"

# Keep the container running
wait
