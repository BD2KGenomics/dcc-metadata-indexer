#python merge_gen_meta.py --storage-access-token `cat ucsc-storage2-client/accessToken` --client-path ucsc-storage2-client --metadata-schema metadata_schema.json --server-host storage2.ucsc-cgl.org

import argparse
import subprocess
import os
import time
import random

def getOptions():

    parser = argparse.ArgumentParser(description='Directory that contains Json files.')
    parser.add_argument('-d', '--test-directory', help='Directory that contains the json metadata files')
    parser.add_argument('-m', '--metadata-schema', help='File that contains the metadata schema')
    parser.add_argument('-s', '--skip-program', help='Lets user skip certain json files that contain a specific program test')
    parser.add_argument('-o', '--only-program', help='Lets user include certain json files that contain a specific program  test')
    parser.add_argument('-r', '--skip-project', help='Lets user skip certain json files that contain a specific program test')
    parser.add_argument('-t', '--only-project', help='Lets user include certain json files that contain a specific program  test')
    parser.add_argument('-a', '--storage-access-token', default="NA", help='Storage access token to download the metadata.json files')
    parser.add_argument('-c', '--client-path', default="ucsc-storage-client/", help='Path to access the ucsc-storage-client tool')
    parser.add_argument('-n', '--server-host', default="storage.ucsc-cgl.org", help='hostname for the storage service')

    args = parser.parse_args()
    return args

def main():
    args = getOptions()

    # main loop for upload
    upload_count = 0
    while True:
        upload_count += 1
        print "LOOP INDEXING: "+str(upload_count)

        # execute indexing
        cmd = "python ../merge_gen_meta.py --storage-access-token %s --client-path %s --metadata-schema %s --server-host %s" % (args.storage_access_token, args.client_path, args.metadata_schema, args.server_host)
        print "CMD: %s" % cmd
        result = subprocess.call(cmd, shell=True)
        if (result != 0):
            print "PROBLEMS INDEXING"

        # now load in ES
        cmd = "curl -XDELETE http://localhost:9200/analysis_index; curl -XPUT http://localhost:9200/analysis_index/_bulk?pretty --data-binary @elasticsearch.jsonl"
        print "CMD: %s" % cmd
        result = subprocess.call(cmd, shell=True)
        if (result != 0):
            print "PROBLEMS LOADING INDEX"

        print "PAUSING..."
        time.sleep(random.randint(5, 10))

        # now do this for the file index
        # execute indexing
        cmd = "python ../Dashboard/file_query.py"
        print "CMD: %s" % cmd
        result = subprocess.call(cmd, shell=True)
        if (result != 0):
            print "PROBLEMS INDEXING"

        # now load in ES
        cmd = "curl -XDELETE http://localhost:9200/analysis_file_index; curl -XPUT 'http://localhost:9200/analysis_file_index' -d @../Dashboard/file_browser/mappings.json; curl -XPUT http://localhost:9200/analysis_file_index/_bulk?pretty --data-binary @elasticsearch.jsonl"
        print "CMD: %s" % cmd
        result = subprocess.call(cmd, shell=True)
        if (result != 0):
            print "PROBLEMS LOADING INDEX"

        # sleep random time before next upload
        print "PAUSING..."
        time.sleep(random.randint(30, 60))

if __name__ == "__main__":
    main()
