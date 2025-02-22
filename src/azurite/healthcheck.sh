#!/bin/bash

container_exists=$(az storage container exists \
    --name test-container \ 
    --connection-string 'DefaultEndpointsProtocol=http;AccountName=datalake;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/datalake' \
    --query "exists" -o tsv)

if [ "$container_exists" == "true" ]; then
    exit 0
else
    exit 1
fi