import luigi
import json
import time
import re
import datetime
import subprocess
from urllib import urlopen
from uuid import uuid4
from elasticsearch import Elasticsearch

# TODO:
# * need a tool that takes bundle ID and file path and converts to file_id for downloading by Dockstore CLI
# * I can fire off a Dockstore CLI to write the data back but who makes a metadata.json?  Should I add this to Dockstore CLI?  Add it here?
#   Probably, for now I can do everything in this task (maybe distinct tasks for download, upload, tool run)
#   and then, over time, push this out to Dockstore CLI.
#   So, effectively, I use Luigi really as a workflow but, ultimately, this will need to move out to worker VMs via Consonance and be done via
#   Dockstore CLI (or Toil) in order to gain flexibility.
# * need to move file outputs used to track running of steps to S3
# * am I doing sufficient error checking?
# * how do I deal with concurrency?  How can I run these concurrently?
# FIXME:
# * this doc makes it sounds like I will need to do some tracking of what's previously been run to prevent duplicates from launching? https://luigi.readthedocs.io/en/stable/tasks.html#dynamic-dependencies
# RUNNING
# $> rm /tmp/foo-*; PYTHONPATH='' luigi --module AlignmentQCTask AlignmentQCCoordinator --es-index-host localhost --es-index-port 9200
# * index builder: 1) needs correct filetype and 2) needs just the filename and not the relative file path (exclude directories)
# rm -rf /tmp/AlignmentQCTask* /tmp/afb54dff-41ad-50e5-9c66-8671c53a278b; PYTHONPATH='' luigi --module AlignmentQCTask AlignmentQCCoordinator --es-index-host localhost --es-index-port 9200 &> log.txt


class AlignmentQCTaskUploader(luigi.Task):
    ucsc_storage_client_path = luigi.Parameter()
    ucsc_storage_host = luigi.Parameter()
    filename = luigi.Parameter(default="filename")
    uuid = luigi.Parameter(default="NA")
    bundle_uuid = luigi.Parameter(default="NA")
    parent_uuid = luigi.Parameter(default="NA")
    tmp_dir = luigi.Parameter(default='/tmp')
    data_dir = luigi.Parameter(default='/tmp/data_dir')
    # just going to generate this UUID
    upload_uuid = str(uuid4())

    def requires(self):
        return AlignmentQCTaskWorker(filepath=self.data_dir+"/"+self.bundle_uuid+"/"+self.filename, ucsc_storage_client_path=self.ucsc_storage_client_path, ucsc_storage_host=self.ucsc_storage_host, uuid=self.uuid, bundle_uuid=self.bundle_uuid, filename=self.filename, parent_uuid=self.parent_uuid, upload_uuid=self.upload_uuid, tmp_dir=self.tmp_dir, data_dir=self.data_dir)

    def run(self):
        print "** UPLOADING **"
        cmd = '''mkdir -p %s/%s/upload/%s %s/%s/manifest/%s && ln -s %s/%s/bamstats_report.zip %s/%s/metadata.json %s/%s/upload/%s && \
echo "Register Uploads:" && \
java -Djavax.net.ssl.trustStore=%s/ssl/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Dserver.baseUrl=%s:8444 -DaccessToken=`cat %s/accessToken` -jar %s/dcc-metadata-client-0.0.16-SNAPSHOT/lib/dcc-metadata-client.jar -i %s/%s/upload/%s -o %s/%s/manifest/%s -m manifest.txt && \
echo "Performing Uploads:" && \
java -Djavax.net.ssl.trustStore=%s/ssl/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Dmetadata.url=%s:8444 -Dmetadata.ssl.enabled=true -Dclient.ssl.custom=false -Dstorage.url=%s:5431 -DaccessToken=`cat %s/accessToken` -jar %s/icgc-storage-client-1.0.14-SNAPSHOT/lib/icgc-storage-client.jar upload --force --manifest %s/%s/manifest/%s/manifest.txt
''' % (self.tmp_dir, self.bundle_uuid, self.upload_uuid, self.tmp_dir, self.bundle_uuid, self.upload_uuid, self.data_dir, self.bundle_uuid, self.tmp_dir, self.bundle_uuid, self.tmp_dir, self.bundle_uuid, self.upload_uuid, self.ucsc_storage_client_path, self.ucsc_storage_host, self.ucsc_storage_client_path, self.ucsc_storage_client_path, self.tmp_dir, self.bundle_uuid, self.upload_uuid, self.tmp_dir, self.bundle_uuid, self.upload_uuid, self.ucsc_storage_client_path, self.ucsc_storage_host, self.ucsc_storage_host, self.ucsc_storage_client_path, self.ucsc_storage_client_path, self.tmp_dir, self.bundle_uuid, self.upload_uuid)
        print "CMD: "+cmd
        result = subprocess.call(cmd, shell=True)
        if result == 0:
            cmd = "rm -rf "+self.data_dir+"/"+self.bundle_uuid+"/bamstats_report.zip "+self.data_dir+"/"+self.bundle_uuid+"/datastore/"
            print "CLEANUP CMD: "+cmd
            result = subprocess.call(cmd, shell=True)
            if result == 0:
                print "CLEANUP SUCCESSFUL"
            f = self.output().open('w')
            print >>f, "uploaded"
            f.close()

    def output(self):
        return luigi.LocalTarget('%s/%s/uploaded' % (self.tmp_dir, self.bundle_uuid))


class AlignmentQCTaskWorker(luigi.Task):
    filepath = luigi.Parameter(default="filepath")
    ucsc_storage_client_path = luigi.Parameter()
    ucsc_storage_host = luigi.Parameter()
    uuid = luigi.Parameter(default="NA")
    bundle_uuid = luigi.Parameter(default="NA")
    filename = luigi.Parameter(default="filename")
    parent_uuid = luigi.Parameter(default="NA")
    upload_uuid = luigi.Parameter(default="NA")
    tmp_dir = luigi.Parameter(default='/tmp')
    data_dir = luigi.Parameter(default='/tmp/data_dir')

    def requires(self):
        return AlignmentQCInputDownloader(ucsc_storage_client_path=self.ucsc_storage_client_path, ucsc_storage_host=self.ucsc_storage_host, filename=self.filename, uuid=self.uuid, bundle_uuid=self.bundle_uuid, tmp_dir=self.tmp_dir, data_dir=self.data_dir)

    def run(self):
        print "** RUNNING REPORT GENERATOR **"

        p = self.output()[0].open('w')
        print >>p, '''{
  "bam_input": {
        "class": "File",
        "path": "%s"
    },
    "bamstats_report": {
        "class": "File",
        "path": "%s"
    }
}''' % (self.data_dir+"/"+self.bundle_uuid+"/"+self.filename, self.data_dir+"/"+self.bundle_uuid+"/bamstats_report.zip")
        p.close()
        # FIXME: docker-machine is likely to break on Linux hosts
        #cmd = "eval $(docker-machine env default); dockstore tool launch --entry quay.io/briandoconnor/dockstore-tool-bamstats:1.25-5 --json %s/%s/params.json" % (self.tmp_dir, self.bundle_uuid)
        cmd = "cd %s && dockstore tool launch --entry quay.io/briandoconnor/dockstore-tool-bamstats:1.25-5 --json %s/%s/params.json" % (self.data_dir+"/"+self.bundle_uuid, self.tmp_dir, self.bundle_uuid)
        print "CMD: "+cmd
        result = subprocess.call(cmd, shell=True)
        print "REPORT GENERATOR RESULT: "+str(result)

        if result == 0:
            # cleanup input
            cmd = "rm "+self.data_dir+"/"+self.bundle_uuid+"/"+self.filename
            print "CLEANUP CMD: "+cmd
            result = subprocess.call(cmd, shell=True)
            if result == 0:
                print "CLEANUP SUCCESSFUL"

            # generate timestamp
            ts_str = datetime.datetime.utcnow().isoformat()

            # now generate a metadata.json which is used for the next upload step
            f = self.output()[1].open('w')
            print >>f, '''{
      "parent_uuids": [
        "%s"
      ],
      "analysis_type": "alignment_qc_report",
      "bundle_uuid": "%s",
      "workflow_name": "quay.io/briandoconnor/dockstore-tool-bamstats",
      "workflow_version": "1.25-5",
      "workflow_outputs": [
       {
        "file_path": "bamstats_report.zip",
        "file_type": "zip"
       }
      ],
      "workflow_inputs" : [
        {
          "file_storage_bundle_uri" : "%s",
          "file_storage_bundle_files" : [
            {
              "file_path": "%s",
              "file_type": "bam",
              "file_storage_uri": "%s"
            }
          ]
        }
      ],
      "timestamp": "%s"
    }''' % (self.parent_uuid, self.upload_uuid, self.bundle_uuid, self.filename, self.uuid, ts_str)
            f.close()

    def output(self):
        return [luigi.LocalTarget('%s/%s/params.json' % (self.tmp_dir, self.bundle_uuid)), luigi.LocalTarget('%s/%s/metadata.json' % (self.tmp_dir, self.bundle_uuid))]


class AlignmentQCInputDownloader(luigi.Task):
    ucsc_storage_client_path = luigi.Parameter()
    ucsc_storage_host = luigi.Parameter()
    filename = luigi.Parameter(default="filename")
    uuid = luigi.Parameter(default="NA")
    bundle_uuid = luigi.Parameter(default="NA")
    tmp_dir = luigi.Parameter(default='/tmp')
    data_dir = luigi.Parameter(default='/tmp/data_dir')

    def run(self):
        print "** DOWNLOADER **"
        cmd = "java -Djavax.net.ssl.trustStore="+self.ucsc_storage_client_path+"/ssl/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Dmetadata.url="+self.ucsc_storage_host+":8444 -Dmetadata.ssl.enabled=true -Dclient.ssl.custom=false -Dstorage.url="+self.ucsc_storage_host+":5431 -DaccessToken=`cat "+self.ucsc_storage_client_path+"/accessToken` -jar "+self.ucsc_storage_client_path+"/icgc-storage-client-1.0.14-SNAPSHOT/lib/icgc-storage-client.jar download --output-dir "+self.data_dir+" --object-id "+self.uuid+" --output-layout bundle"
        print cmd
        result = subprocess.call(cmd, shell=True)
        print "DOWNLOAD RESULT: "+str(result)
        if result == 0:
            p = self.output().open('w')
            print >>p, "finished downloading"
            p.close()

    def output(self):
        return luigi.LocalTarget(self.tmp_dir+"/"+self.bundle_uuid+"/"+self.filename)


class AlignmentQCCoordinator(luigi.Task):

    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    ucsc_storage_client_path = luigi.Parameter(default='../ucsc-storage-client')
    ucsc_storage_host = luigi.Parameter(default='https://storage2.ucsc-cgl.org')
    tmp_dir = luigi.Parameter(default='/tmp')
    data_dir = luigi.Parameter(default='/tmp/data_dir')
    max_jobs = luigi.Parameter(default='1')
    bundle_uuid_filename_to_file_uuid = {}

    def requires(self):
        print "** COORDINATOR **"
        # now query the metadata service so I have the mapping of bundle_uuid & file names -> file_uuid
        json_str = urlopen(str(self.ucsc_storage_host+":8444/entities?page=0")).read()
        metadata_struct = json.loads(json_str)
        print "** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"])
        for i in range(0, metadata_struct["totalPages"]):
            print "** CURRENT METADATA TOTAL PAGES: "+str(i)
            json_str = urlopen(str(self.ucsc_storage_host+":8444/entities?page="+str(i))).read()
            metadata_struct = json.loads(json_str)
            for file_hash in metadata_struct["content"]:
                self.bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]

        # now query elasticsearch
        es = Elasticsearch([{'host': self.es_index_host, 'port': self.es_index_port}])
        # see jqueryflag_alignment_qc
        # curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @jqueryflag_alignment_qc
        res = es.search(index="analysis_index", body={"query":{"filtered":{"filter":{"bool":{"must":{"or":[{"terms":{"flags.normal_alignment_qc_report":["false"]}},{"terms":{"flags.tumor_alignment_qc_report":["false"]}}]}}},"query":{"match_all":{}}}}}, size=5000)

        listOfJobs = []

        print("Got %d Hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:
            print("%(donor_uuid)s %(center_name)s %(project)s" % hit["_source"])
            for specimen in hit["_source"]["specimen"]:
                for sample in specimen["samples"]:
                    for analysis in sample["analysis"]:
                        print "MAYBE HIT??? "+analysis["analysis_type"]+" "+str(hit["_source"]["flags"]["normal_alignment_qc_report"])+" "+specimen["submitter_specimen_type"]
                        if analysis["analysis_type"] == "alignment" and (hit["_source"]["flags"]["normal_alignment_qc_report"] == False and re.match("^Normal - ", specimen["submitter_specimen_type"])) or (hit["_source"]["flags"]["tumor_alignment_qc_report"] == False and re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", specimen["submitter_specimen_type"])):
                            print "HIT!!!! "+analysis["analysis_type"]+" "+str(hit["_source"]["flags"]["normal_alignment_qc_report"])+" "+specimen["submitter_specimen_type"]
                            bamFile = ""
                            for file in analysis["workflow_outputs"]:
                                if (file["file_type"] == "bam"):
                                    bamFile = file["file_path"]
                            if len(listOfJobs) < int(self.max_jobs):
                                listOfJobs.append(AlignmentQCTaskUploader(ucsc_storage_client_path=self.ucsc_storage_client_path, ucsc_storage_host=self.ucsc_storage_host, filename=bamFile, uuid=self.fileToUUID(bamFile, analysis["bundle_uuid"]), bundle_uuid=analysis["bundle_uuid"], parent_uuid=sample["sample_uuid"], tmp_dir=self.tmp_dir, data_dir=self.data_dir))

        # these jobs are yielded to
        return listOfJobs

    def run(self):
        # now make a final report
        f = self.output().open('w')
        print >>f, "batch is complete"
        f.close()

    def output(self):
        # the final report
        ts = time.time()
        ts_str = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')
        return luigi.LocalTarget('%s/AlignmentQCTask-%s.txt' % (self.tmp_dir, ts_str))

    def fileToUUID(self, input, bundle_uuid):
        return self.bundle_uuid_filename_to_file_uuid[bundle_uuid+"_"+input]
        #"afb54dff-41ad-50e5-9c66-8671c53a278b"

if __name__ == '__main__':
    luigi.run()
