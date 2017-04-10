#!/bin/bash

#Exit if an error is detected
#set -o errexit
sleep 15
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

#Go into the indexer folder
cd /app/dcc-metadata-indexer

#Run the metadata-indexer
python metadata_indexer_v2.py ${ARGUMENTS[@]}

echo "Updating analysis_index"
curl -XDELETE http://$esService:9200/analysis_buffer/
curl -XPUT http://$esService:9200/analysis_buffer/ -d @analysis_settings.json
curl -XPUT http://$esService:9200/analysis_buffer/_mapping/meta?update_all_types  -d @mapping.json
curl -XPUT http://$esService:9200/analysis_buffer/_bulk?pretty --data-binary @elasticsearch.jsonl

#####Change buffer to point to alias
curl -XPOST http://$esService:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "analysis_real", "alias" : "analysis_index" } }, { "add" : { "index" : "analysis_buffer", "alias" : "analysis_index" } } ] }'

##Update real index analysis
curl -XDELETE http://$esService:9200/analysis_real/
curl -XPUT http://$esService:9200/analysis_real/ -d @analysis_settings.json
curl -XPUT http://$esService:9200/analysis_real/_mapping/meta?update_all_types  -d @mapping.json
curl -XPUT http://$esService:9200/analysis_real/_bulk?pretty --data-binary @elasticsearch.jsonl

#Change alias one last time from buffer to real
curl -XPOST http://$esService:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "analysis_buffer", "alias" : "analysis_index" } }, { "add" : { "index" : "analysis_real", "alias" : "analysis_index" } } ] }'
echo "Starting es_filebrowser_index_v2.py; Creating fb_index"
#Run the 
sleep 6
python es_filebrowser_index_v2.py --access public --repoBaseUrl $storageHost --repoCountry US --repoName Redwood-AWS-Oregon --repoOrg UCSC --repoType Redwood --repoCountry US >> /app/dcc-metadata-indexer/es-jsonls/log.txt  2>&1

echo "Updating fb_index"
curl -XDELETE http://$esService:9200/fb_buffer/
curl -XPUT http://$esService:9200/fb_buffer/ -d @fb_settings.json
curl -XPUT http://$esService:9200/fb_buffer/_mapping/meta?update_all_types  -d @fb_mapping.json
curl -XPUT http://$esService:9200/fb_buffer/_bulk?pretty --data-binary @fb_index.jsonl

###Change alias to point to fb_buffer
curl -XPOST http://$esService:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "fb_index", "alias" : "fb_alias" } }, { "add" : { "index" : "fb_buffer", "alias" : "fb_alias" } } ] }'

####Index/Update the data in fb_index
curl -XDELETE http://$esService:9200/fb_index/
curl -XPUT http://$esService:9200/fb_index/ -d @fb_settings.json
curl -XPUT http://$esService:9200/fb_index/_mapping/meta?update_all_types  -d @fb_mapping.json
curl -XPUT http://$esService:9200/fb_index/_bulk?pretty --data-binary @fb_index.jsonl

#Change alias one last time
curl -XPOST http://$esService:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "fb_buffer", "alias" : "fb_alias" } }, { "add" : { "index" : "fb_index", "alias" : "fb_alias" } } ] }'

###NOW HANDLE CREATING THE NEW BILLINGINDEX###
#python metadata_indexer_v2.py -preserve-version --skip-program TEST --skip-project TEST  --storage-access-token $storageAccessToken  --metadata-schema $metadataSchema --server-host $storageHost
python metadata_indexer_v2.py -preserve-version ${ARGUMENTS[@]}

#Populate the billing index
#Change the Buffer index
echo "Updating billing_idx"
curl -XDELETE http://$esService:9200/billing_buffer/
curl -XPUT http://$esService:9200/billing_buffer/
curl -XPUT http://$esService:9200/billing_buffer/_mapping/meta?update_all_types  -d @billing_mapping.json
curl -XPUT http://$esService:9200/billing_buffer/_bulk?pretty --data-binary @duped_elasticsearch.jsonl

#Change aliases
curl -XPOST http://$esService:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "billing_real", "alias" : "billing_idx" } }, { "add" : { "index" : "billing_buffer", "alias" : "billing_idx" } } ] }'

#Update real billing_index

curl -XDELETE http://$esService:9200/billing_real/
curl -XPUT http://$esService:9200/billing_real/
curl -XPUT http://$esService:9200/billing_real/_mapping/meta?update_all_types  -d @billing_mapping.json
curl -XPUT http://$esService:9200/billing_real/_bulk?pretty --data-binary @duped_elasticsearch.jsonl

#Change the alias again, so that billing_idx points again to the real billing_real index
curl -XPOST http://$esService:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "billing_buffer", "alias" : "billing_idx" } }, { "add" : { "index" : "billing_real", "alias" : "billing_idx" } } ] }'

#Generate billing reports. 
echo 'Generating billings'
sleep 6
python generate_billings.py
#This moves all the .jsonl files to the es-jsonls folder (easier to mount only the jsonl files as opposed to everything else.)
#find . -name "*.jsonl" -exec cp {} /app/dcc-metadata-indexer/es-jsonls \;
\cp /app/dcc-metadata-indexer/*jsonl /app/dcc-metadata-indexer/es-jsonls

echo "Removing existing validated.jsonl.gz and fb_index.jsonl.gz"
rm /app/dcc-metadata-indexer/es-jsonls/validated.jsonl.gz
rm /app/dcc-metadata-indexer/es-jsonls/fb_index.jsonl.gz

echo "Gzip validated.jsonl and fb_index.jsonl" 
gzip /app/dcc-metadata-indexer/es-jsonls/validated.jsonl
gzip /app/dcc-metadata-indexer/es-jsonls/fb_index.jsonl

#curl -XGET http://$esService:9200/_cat/health
#Make ownership to that of the executer of the docker image. 
user=$USER_GROUP
if [[ ! -z "$user" ]]
then  
  chown -R ${user} /app/dcc-metadata-indexer/es-jsonls/
  chown -R ${user} /app/dcc-metadata-indexer/endpoint_metadata/
  chown -R ${user} /app/dcc-metadata-indexer/redacted/
  chown -R ${user} /app/dcc-metadata-indexer/logs/
fi

#Now set/reset the cronjob if a flag is passed
if [[ ! -z "$cronJob" ]]
then
  echo "Setting the cron job"
  #Used this for the cronjob: https://gist.github.com/mhubig/a01276e17496e9fd6648cf426d9ceeec
  #env > /app/env.txt && crontab /etc/cron.d/indexer-cron && /usr/sbin/crond -f -d 0
  env > /root/env.txt && cron -f -L 15 && echo "Cron Job set"
 while true; do sleep 10000; done
fi
