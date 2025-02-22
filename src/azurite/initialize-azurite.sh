#!/bin/bash

# Wait for Azurite to be available
while ! az storage container list --connection-string 'DefaultEndpointsProtocol=http;AccountName=datalake;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/datalake'; do
    echo "Waiting for Azurite..."
    sleep 2
done

echo "##############################################"
echo "Creating default containers ..."
echo "##############################################"

# Create the blob container
az storage container create \
    --name test-container \
    --connection-string 'DefaultEndpointsProtocol=http;AccountName=datalake;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/datalake'


echo "##############################################"
echo "test-container created successfully ..."
echo "##############################################"

tail -f /dev/null