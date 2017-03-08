#!/bin/bash
#now=$(date +"%T")
#Add shell script code

set -o errexit

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
-preserve-version Keep all copies of analysis events 
"
}
#Add default args to ARGS
clientPath='/app/redwood-client/ucsc-storage-client/'
metadataSchema='/app/dcc-metadata-indexer/metadata_schema.json'
declare -A ARGS
ARGS["--client-path"]=$clientPath
ARGS["--metadata-schema"]=$metadataSchema
#ARGS=()
ARGUMENTS=()
#declare -A ARGS
empty_arg(){
    if [[ -z "$1" ]]
    then
        echo "Missing value. Please enter a non-empty value"
        exit
    fi
    #ARGS+=($2 $1)
    ARGS[$2]=$1
    #echo ${ARGS[@]}
}

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
        -preserve-version)
            empty_arg $2 $1
            preserveVersion $2
            ;;
    esac
    shift
done


for key in ${!ARGS[@]}; do
    ARGUMENTS+=(${key} ${ARGS[${key}]})
done
python metadata_indexer.py ${ARGUMENTS[@]}

#Still need to insert the code to copy the jsonl files to the appropriate folder so they are available for other containers.
