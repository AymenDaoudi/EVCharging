#!/bin/bash

set -eo pipefail

if curl -s -u admin:pass http://localhost:8080/db/Db >/dev/null 2>&1; then
    echo "Health check succeeded!"
    exit 0
else
    echo "Health check failed!"
    exit 1
fi