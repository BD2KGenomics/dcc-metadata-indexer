#!/bin/bash

#Exit if an error is detected
set -o errexit

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
clientPath(){
    clientPath=$1
}
preserveVersion(){
    preserveVersion=$1
}
maxPages(){
    maxPages=$1
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
--client-path | -c Path to access the ucsc-storage-client tool
--max-pages | -p Specify maximum number of pages to download
-preserve-version Keep all copies of analysis events 
"
}

#Declare the associative array i.e. a dictionary
declare -A ARGS

#Add default args to ARGS
clientPath='/app/redwood-client/ucsc-storage-client/'
metadataSchema='/app/dcc-metadata-indexer/metadata_schema.json'

#Assign the key and variable pair in the associative array (Dictionary)
ARGS["--client-path"]=$clientPath
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
        --client-path | -c)
            empty_arg $2 $1
            clientPath $2
            ;;
        --max-pages | -p)
            empty_arg $2 $1
            maxPages $2
            ;;
        -preserve-version)
            empty_arg $2 $1
            preserveVersion $2
            ;;
    esac
    shift
done

#Add the arguments for the metadata-indexer.py
for key in ${!ARGS[@]}; do
    ARGUMENTS+=(${key} ${ARGS[${key}]})
done

#Run the metadata-indexer
python metadata_indexer.py ${ARGUMENTS[@]}

echo "Updating analysis_index"
curl -XDELETE http://elasticsearch1:9200/analysis_buffer/
curl -XPUT http://elasticsearch1:9200/analysis_buffer/ -d @analysis_settings.json
curl -XPUT http://elasticsearch1:9200/analysis_buffer/_mapping/meta?update_all_types  -d @mapping.json
curl -XPUT http://elasticsearch1:9200/analysis_buffer/_bulk?pretty --data-binary @elasticsearch.jsonl

#####Change buffer to point to alias
curl -XPOST http://elasticsearch1:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "analysis_real", "alias" : "analysis_index" } }, { "add" : { "index" : "analysis_buffer", "alias" : "analysis_index" } } ] }'

##Update real index analysis
curl -XDELETE http://elasticsearch1:9200/analysis_real/
curl -XPUT http://elasticsearch1:9200/analysis_real/ -d @analysis_settings.json
curl -XPUT http://elasticsearch1:9200/analysis_real/_mapping/meta?update_all_types  -d @mapping.json
curl -XPUT http://elasticsearch1:9200/analysis_real/_bulk?pretty --data-binary @elasticsearch.jsonl

#Change alias one last time from buffer to real
curl -XPOST http://elasticsearch1:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "analysis_buffer", "alias" : "analysis_index" } }, { "add" : { "index" : "analysis_real", "alias" : "analysis_index" } } ] }'
echo "Starting es_filebrowser_index.py; Creating fb_index"
#Run the 
python es_filebrowser_index.py --access public --repoBaseUrl storage.ucsc-cgl.org --repoCountry US --repoName Redwood-AWS-Oregon --repoOrg UCSC --repoType Redwood --repoCountry US

echo "Updating fb_index"
curl -XDELETE http://elasticsearch1:9200/fb_buffer/
curl -XPUT http://elasticsearch1:9200/fb_buffer/ -d @fb_settings.json
curl -XPUT http://elasticsearch1:9200/fb_buffer/_mapping/meta?update_all_types  -d @fb_mapping.json
curl -XPUT http://elasticsearch1:9200/fb_buffer/_bulk?pretty --data-binary @fb_index.jsonl

###Change alias to point to fb_buffer
curl -XPOST http://elasticsearch1:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "fb_index", "alias" : "fb_alias" } }, { "add" : { "index" : "fb_buffer", "alias" : "fb_alias" } } ] }'

####Index/Update the data in fb_index
curl -XDELETE http://elasticsearch1:9200/fb_index/
curl -XPUT http://elasticsearch1:9200/fb_index/ -d @fb_settings.json
curl -XPUT http://elasticsearch1:9200/fb_index/_mapping/meta?update_all_types  -d @fb_mapping.json
curl -XPUT http://elasticsearch1:9200/fb_index/_bulk?pretty --data-binary @fb_index.jsonl

#Change alias one last time
curl -XPOST http://elasticsearch1:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "fb_buffer", "alias" : "fb_alias" } }, { "add" : { "index" : "fb_index", "alias" : "fb_alias" } } ] }'

###NOW HANDLE CREATING THE NEW BILLINGINDEX###
python metadata_indexer.py -preserve-version --skip-program TEST --skip-project TEST  --storage-access-token $storageAccessToken --client-path $clientPath --metadata-schema $metadataSchema --server-host $storageHost

#Populate the billing index
#Change the Buffer index
echo "Updating billing_idx"
curl -XDELETE http://elasticsearch1:9200/billing_buffer/
curl -XPUT http://elasticsearch1:9200/billing_buffer/
curl -XPUT http://elasticsearch1:9200/billing_buffer/_mapping/meta?update_all_types  -d @billing_mapping.json
curl -XPUT http://elasticsearch1:9200/billing_buffer/_bulk?pretty --data-binary @duped_elasticsearch.jsonl

#Change aliases
curl -XPOST http://elasticsearch1:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "billing_real", "alias" : "billing_idx" } }, { "add" : { "index" : "billing_buffer", "alias" : "billing_idx" } } ] }'

#Update real billing_index

curl -XDELETE http://elasticsearch1:9200/billing_real/
curl -XPUT http://elasticsearch1:9200/billing_real/
curl -XPUT http://elasticsearch1:9200/billing_real/_mapping/meta?update_all_types  -d @billing_mapping.json
curl -XPUT http://elasticsearch1:9200/billing_real/_bulk?pretty --data-binary @duped_elasticsearch.jsonl

#Change the alias again, so that billing_idx points again to the real billing_real index
curl -XPOST http://elasticsearch1:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "billing_buffer", "alias" : "billing_idx" } }, { "add" : { "index" : "billing_real", "alias" : "billing_idx" } } ] }'

#TODO: add code to do the daily report generation

#This moves all the .jsonl files to the es-jsonls folder (easier to mount only the jsonl files as opposed to everything else.)
#find . -name "*.jsonl" -exec cp {} /app/dcc-metadata-indexer/es-jsonls \;
\cp /app/dcc-metadata-indexer/*jsonl /app/dcc-metadata-indexer/es-jsonls

#curl -XGET http://elasticsearch1:9200/_cat/health
#Make ownership to that of the executer of the docker image. 
user=$USER_GROUP
if [[ ! -z "$user" ]]
then  
  chown -R ${user} /app/dcc-metadata-indexer/es-jsonls/
  chown -R ${user} /app/dcc-metadata-indexer/endpoint_metadata/
  chown -R ${user} /app/dcc-metadata-indexer/redacted/
fi

