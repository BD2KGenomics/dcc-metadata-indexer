import argparse
import subprocess
import os
import time
import random

def getOptions():

    parser = argparse.ArgumentParser(description='Directory that contains Json files.')

    parser.add_argument("-b", "--bam-url", default="https://s3.amazonaws.com/oconnor-test-bucket/sample-data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam", help="Switch for verbose mode.")
    parser.add_argument("-i", "--input-metadata-schema", default="input_metadata.json", help="flattened json schema file for input metadata")
    parser.add_argument("-m", "--metadata-schema", default="metadata_schema.json", help="flattened json schema file for metadata")
    parser.add_argument("-d", "--output-dir", default="output_metadata", help="output directory. In the case of colliding file names, the older file will be overwritten.")
    parser.add_argument("-r", "--receipt-file", default="receipt.tsv", help="receipt file name. This tsv file is the receipt of the upload, with UUIDs filled in.")
    parser.add_argument("--storage-access-token", default="NA", help="access token for storage system looks something like 12345678-abcd-1234-abcdefghijkl.")
    parser.add_argument("--metadata-server-url", default="https://storage.ucsc-cgl.org:8444", help="URL for metadata server.")
    parser.add_argument("--storage-server-url", default="https://storage.ucsc-cgl.org:5431", help="URL for storage server.")
    parser.add_argument("--ucsc-storage-client-path", default="ucsc-storage-client", help="Location of client.")

    args = parser.parse_args()
    return args

def main():
    args = getOptions()
    # prep download
    url_arr = args.bam_url.split("/")
    if not os.path.isfile(url_arr[-1]):
        cmd = "curl -k %s > %s" % (args.bam_url, url_arr[-1])
        print "DOWNLOADING: "+cmd
        result = subprocess.call(cmd, shell=True)
        if (result != 0):
            print "PROBLEMS DOWNLOADING"
    # main loop for upload
    upload_count = 0
    while True:
        upload_count += 1
        print "LOOP UPLOAD: "+str(upload_count)
        # create template
        specimen = '{0:05}'.format(random.randint(1, 1000000))
        template = '''Program	Project	Center Name	Submitter Donor ID	Donor UUID	Submitter Specimen ID	Specimen UUID	Submitter Specimen Type	Submitter Sample ID	Sample UUID	Analysis Type	Workflow Name	Workflow Version	File Type	File Path	Upload File ID	Data Bundle ID	Metadata.json
Demo	Demo	UCSC	S%s		S%sa		Normal - blood derived	S%sa1		alignment	bwa-mem-aligner	1.0.0	bam	NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam''' % (str(specimen), str(specimen), str(specimen))
        f = open('sample.tsv', 'w')
        print >>f, template
        f.close()
        # execute upload
        cmd = "rm -rf output_metadata; mkdir -p output_metadata; python ../generate_metadata.py --input-metadata-schema %s --metadata-schema %s --output-dir output_metadata --receipt-file receipt.tsv --storage-access-token %s --metadata-server-url %s --storage-server-url %s --force-upload --ucsc-storage-client-path %s sample.tsv" % (args.input_metadata_schema, args.metadata_schema, args.storage_access_token, args.metadata_server_url, args.storage_server_url, args.ucsc_storage_client_path)
        print "CMD: %s" % cmd
        result = subprocess.call(cmd, shell=True)
        if (result != 0):
            print "PROBLEMS UPLOADING"
        # sleep random time before next upload
        print "PAUSING..."
        time.sleep(random.randint(30, 60))

if __name__ == "__main__":
    main()
