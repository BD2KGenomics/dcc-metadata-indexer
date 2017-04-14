#!/bin/bash
echo "The date is:"
date
echo "Starting run2 script..."
/app/dcc-metadata-indexer/run2.sh --storage-access-token $REDWOOD_ACCESS_TOKEN --server-host $REDWOOD_SERVER --skip-uuid-directory /app/dcc-metadata-indexer/redacted --skip-program TEST --skip-project TEST --es-service $ES_SERVICE > /app/dcc-metadata-indexer/logs/log.txt  2>&1
