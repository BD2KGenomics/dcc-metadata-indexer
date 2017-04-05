#!/bin/bash

#Exit if an error is detected
#set -o errexit
#sleep 15
#Functions for assigning variables from flags
storageAccessToken(){
    storageAccessToken=$1
}
serverHost(){
    storageHost=$1
}
skipUuidDirectory(){
    skipUuidDirectory=$1
}
testDirectory(){
    testDirectory=$1
}
metadataSchema(){
    metadataSchema=$1
}
skipProgram(){
    skipProgram=$1
}
onlyProgram(){
    onlyProgram=$1
}
skipProject(){
    skipProject=$1
}
onlyProject(){
    onlyProject=$1
}
preserveVersion(){
    preserveVersion=$1
}
maxPages(){
    maxPages=$1
}
esService(){
    esService=$1
}
helpmenu(){
    echo "--help | -h for help 
--storage-access-token | -a for the access token 
--server-host | -n for hostname for the storage service 
--skip-uuid-directory | -u Directory that contains files with file uuids (bundle uuids, one per line, file ending with .redacted) that represent databundles that should be skipped, useful for redacting content (but not deleting it)
--test-directory | -d Directory that contains the json metadata files
--metadata-schema | -m File that contains the metadata schema
--skip-program | -s Lets user skip certain json files that contain a specific program test
--only-program | -o Lets user include certain json files that contain a specific program  test
--skip-project | -r Lets user skip certain json files that contain a specific program test
--only-project | -t Lets user include certain json files that contain a specific program  test
--max-pages | -p Specify maximum number of pages to download
-preserve-version Keep all copies of analysis events 
--es-service | -e Specify the name of the host for the elasticsearch service.
--cron-job | -j Include this flag to run the whole indexing process as a cron job
"
}

#Declare the associative array i.e. a dictionary
declare -A ARGS

#Add default args to ARGS
metadataSchema='/app/dcc-metadata-indexer/metadata_schema.json'
esService=elasticsearch1

#Assign the key and variable pair in the associative array (Dictionary)
#ARGS["--client-path"]=$clientPath
ARGS["--metadata-schema"]=$metadataSchema

#Declare array with the arguments for the metadata-indexer.py
ARGUMENTS=()

#Assign the arguments in a dictionary; Exit if flag was empty
empty_arg(){
    if [[ -z "$1" ]]
    then
        echo "Missing value. Please enter a non-empty value"
        exit
    fi
    ARGS[$2]=$1
}

#Assign variables
while [ ! $# -eq 0 ]
do
    case "$1" in
        --help | -h)
            helpmenu
            exit
            ;;
        --storage-access-token | -a)
            empty_arg $2 $1
            storageAccessToken $2
            ;;
        --server-host | -n)
            empty_arg $2 $1
            serverHost $2
            ;;
        --skip-uuid-directory | -u)
            empty_arg $2 $1
            skipUuidDirectory $2
            ;;
        --test-directory | -d)
            empty_arg $2 $1
            testDirectory $2
            ;;
        --metadata-schema | -m)
            empty_arg $2 $1
            metadataSchema $2
            ;;
        --skip-program | -s)
            empty_arg $2 $1
            skipProgram $2
            ;;
        --only-program | -o)
            empty_arg $2 $1
            onlyProgram $2
            ;;
        --skip-project | -r)
            empty_arg $2 $1
            skipProject $2
            ;;
        --only-project | -t)
            empty_arg $2 $1
            onlyProject $2
            ;;
        --max-pages | -p)
            empty_arg $2 $1
            maxPages $2
            ;;
        -preserve-version)
            empty_arg $2 $1
            preserveVersion $2
            ;;
        --es-service | -e)
            esService $2
            ;;
        --cron-job | -j)
            cronJob=1
            ;;
    esac
    shift
done

#Add the arguments for the metadata-indexer.py
for key in ${!ARGS[@]}; do
    ARGUMENTS+=(${key} ${ARGS[${key}]})
done

echo "Access token in REDWOOD_ACCESS_TOKEN: "$REDWOOD_ACCESS_TOKEN
echo "Redwood server is REDWOOD_SERVER: "$REDWOOD_SERVER
echo "ES_SERVICE is :"$ES_SERVICE
echo "esService is :"$esService
echo "storageHost is :"$storageHost 

echo "Testing analysis_index health check"
curl -XGET http://$esService:9200/_cluster/health/analysis_index?wait_for_status=green&timeout=10s&pretty=true

echo "Testing fb_alias health check"
curl -XGET http://$esService:9200/_cluster/health/fb_alias?wait_for_status=green&timeout=10s&pretty=true
echo "Printing env"
env
echo "That's it. End of script"

