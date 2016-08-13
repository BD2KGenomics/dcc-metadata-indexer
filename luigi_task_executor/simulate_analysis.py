import argparse
import subprocess
import os
import time
import random

# TODO: need to paramerterize the bam file used
# TODO: some hardcoded paths below

def getOptions():

    parser = argparse.ArgumentParser(description='Tool that runs analysis continuously.')
    parser.add_argument('-e', '--es-index-host', help='elasticsearch host name')
    parser.add_argument('-p', '--es-index-port', help='elasticsearch port')
    parser.add_argument('-c', '--ucsc-storage-client-path', help='Client path')
    parser.add_argument('-s', '--ucsc-storage-host', help='Storage host.')

    args = parser.parse_args()
    return args

def main():
    args = getOptions()
    # main loop for upload
    upload_count = 0
    while True:
        upload_count += 1
        print "LOOP ANALYSIS: "+str(upload_count)

        cmd = "mkdir -p /mnt/AlignmentQCTask && PYTHONPATH='' luigi --module AlignmentQCTask AlignmentQCCoordinator --es-index-host %s --es-index-port %s --ucsc-storage-client-path %s --ucsc-storage-host %s --tmp-dir `pwd`/luigi_state --data-dir /mnt/AlignmentQCTask --max-jobs 100" % (args.es_index_host, args.es_index_port, args.ucsc_storage_client_path, args.ucsc_storage_host)
        print "CMD: %s" % cmd
        result = subprocess.call(cmd, shell=True)
        if (result != 0):
            print "PROBLEMS RUNNING WORKFLOW"
        print "PAUSING..."
        time.sleep(30)

if __name__ == "__main__":
    main()
