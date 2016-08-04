import luigi
import json
import time
import re
import datetime
from elasticsearch import Elasticsearch

# TODO:
# * need a tool that takes bundle ID and file path and converts to file_id for downloading by Dockstore CLI
# * I can fire off a Dockstore CLI to write the data back but who makes a metadata.json?  Should I add this to Dockstore CLI?  Add it here?
#   Probably, for now I can do everything in this task (maybe distinct tasks for download, upload, tool run)
#   and then, over time, push this out to Dockstore CLI.
#   So, effectively, I use Luigi really as a workflow but, ultimately, this will need to move out to worker VMs via Consonance and be done via
#   Dockstore CLI (or Toil) in order to gain flexibility.
# * need to move file outputs used to track running of steps to S3
# FIXME:
# * this doc makes it sounds like I will need to do some tracking of what's previously been run to prevent duplicates from launching? https://luigi.readthedocs.io/en/stable/tasks.html#dynamic-dependencies
# RUNNING
# $> rm /tmp/foo-*; PYTHONPATH='' luigi --module AlignmentQCTask AlignmentQCCoordinator --es-index-host localhost --es-index-port 9200
# * index builder: 1) needs correct filetype and 2) needs just the filename and not the relative file path (exclude directories)
# rm -rf /tmp/AlignmentQCTask* /tmp/afb54dff-41ad-50e5-9c66-8671c53a278b; PYTHONPATH='' luigi --module AlignmentQCTask AlignmentQCCoordinator --es-index-host localhost --es-index-port 9200 &> log.txt


class AlignmentQCTaskUploader(luigi.Task):
    uuid = luigi.Parameter(default="NA")
    bundle_uuid = luigi.Parameter(default="NA")
    filename = luigi.Parameter(default="filename")
    ucsc_storage_client_path = luigi.Parameter()
    ucsc_storage_host = luigi.Parameter()

    def requires(self):
        return AlignmentQCTaskWorker(filepath="/tmp/"+self.uuid+"/"+self.filename, ucsc_storage_client_path=self.ucsc_storage_client_path, ucsc_storage_host=self.ucsc_storage_host, uuid=self.uuid, bundle_uuid=self.bundle_uuid, filename=self.filename)

    def run(self):
        print "** UPLOADING **"
        print "java -Djavax.net.ssl.trustStore="+self.ucsc_storage_client_path+"/ssl/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Dmetadata.url="+self.ucsc_storage_host+":8444 -Dmetadata.ssl.enabled=true -Dclient.ssl.custom=false -Dstorage.url="+self.ucsc_storage_host+":5431 -DaccessToken=`cat "+self.ucsc_storage_client_path+"/accessToken` -jar "+self.ucsc_storage_client_path+"/icgc-storage-client-1.0.14-SNAPSHOT/lib/icgc-storage-client.jar download --output-dir /tmp --object-id "+self.uuid+" --output-layout filename"
        time.sleep(5)
        f = self.output().open('w')
        print >>f, "uploaded"
        f.close()

    def output(self):
        return luigi.LocalTarget('/tmp/%s/metadata.json.uploaded' % self.uuid)


class AlignmentQCTaskWorker(luigi.Task):
    filepath = luigi.Parameter(default="filepath")
    ucsc_storage_client_path = luigi.Parameter()
    ucsc_storage_host = luigi.Parameter()
    uuid = luigi.Parameter(default="NA")
    bundle_uuid = luigi.Parameter(default="NA")
    filename = luigi.Parameter(default="filename")

    def requires(self):
        return AlignmentQCInputDownloader(ucsc_storage_client_path=self.ucsc_storage_client_path, ucsc_storage_host=self.ucsc_storage_host, filename=self.filename, uuid=self.uuid, bundle_uuid=self.bundle_uuid)

    def run(self):
        print "** RUNNING REPORT GENERATOR **"

        p = self.output()[0].open('w')
        print >>p, "{params}"
        p.close()

        print "dockstore tool ..."
        time.sleep(5)

        # now generate a metadata.json which is used for the next step
        f = self.output()[1].open('w')
        print >>f, "{json}"
        f.close()

        #yield AlignmentQCTaskUploader(metadata="/tmp/"+self.uuid+"/metadata.json", report="/tmp/"+self.uuid+"/alignment_qc_report.zip", ucsc_storage_client_path=self.ucsc_storage_client_path, ucsc_storage_host=self.ucsc_storage_host, uuid=self.uuid, filename=self.filename)

    def output(self):
        return [luigi.LocalTarget('/tmp/%s/params.json' % self.uuid), luigi.LocalTarget('/tmp/%s/metadata.json' % self.uuid)]


class AlignmentQCInputDownloader(luigi.Task):
    uuid = luigi.Parameter(default="NA")
    bundle_uuid = luigi.Parameter(default="NA")
    filename = luigi.Parameter(default="filename")
    ucsc_storage_client_path = luigi.Parameter()
    ucsc_storage_host = luigi.Parameter()

    def run(self):
        print "** DOWNLOADER **"
        print "java -Djavax.net.ssl.trustStore="+self.ucsc_storage_client_path+"/ssl/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Dmetadata.url="+self.ucsc_storage_host+":8444 -Dmetadata.ssl.enabled=true -Dclient.ssl.custom=false -Dstorage.url="+self.ucsc_storage_host+":5431 -DaccessToken=`cat "+self.ucsc_storage_client_path+"/accessToken` -jar "+self.ucsc_storage_client_path+"/icgc-storage-client-1.0.14-SNAPSHOT/lib/icgc-storage-client.jar download --output-dir /tmp --object-id "+self.uuid+" --output-layout bundle"
        time.sleep(5)
        #yield AlignmentQCTaskWorker(filepath="/tmp/"+self.uuid+"/"+self.filename, ucsc_storage_client_path=self.ucsc_storage_client_path, ucsc_storage_host=self.ucsc_storage_host, uuid=self.uuid, filename=self.filename)

        # now make a final report
        f = self.output().open('w')
        print >>f, "download is complete"
        f.close()

    def output(self):
        return luigi.LocalTarget("/tmp/"+self.uuid+"/"+self.filename)


class AlignmentQCCoordinator(luigi.Task):

    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    ucsc_storage_client_path = luigi.Parameter(default='../ucsc-storage-client')
    ucsc_storage_host = luigi.Parameter(default='https://storage2.ucsc-cgl.org')

    def requires(self):
        print "** COORDINATOR **"
        es = Elasticsearch([{'host': self.es_index_host, 'port': self.es_index_port}])
        # see jqueryflag_alignment_qc
        # curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @jqueryflag_alignment_qc
        res = es.search(index="analysis_index", body={"query":{"filtered":{"filter":{"bool":{"must":{"or":[{"terms":{"flags.normal_alignment_qc_report":["false"]}},{"terms":{"flags.tumor_alignment_qc_report":["false"]}}]}}},"query":{"match_all":{}}}}})

        listOfJobs = []

        print("Got %d Hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:
            print("%(donor_uuid)s %(center_name)s %(project)s" % hit["_source"])
            for specimen in hit["_source"]["specimen"]:
                for sample in specimen["samples"]:
                    for analysis in sample["analysis"]:
                        if analysis["analysis_type"] == "alignment" and (hit["_source"]["flags"]["normal_alignment_qc_report"] == False and re.match("^Normal - ", specimen["submitter_specimen_type"])) or (hit["_source"]["flags"]["tumor_alignment_qc_report"] == False and re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", specimen["submitter_specimen_type"])):
                            print "HIT!!!! "+analysis["analysis_type"]+" "+str(hit["_source"]["flags"]["normal_alignment_qc_report"])+" "+specimen["submitter_specimen_type"]
                            bamFile = ""
                            for file in analysis["workflow_outputs"]:
                                if (file["file_type"] == "bam"):
                                    bamFile = file["file_path"]
                            #listOfJobs.append(AlignmentQCInputDownloader(ucsc_storage_client_path=self.ucsc_storage_client_path, ucsc_storage_host=self.ucsc_storage_host, filename=bamFile, uuid=self.fileToUUID(bamFile, analysis["bundle_uuid"])))
                            listOfJobs.append(AlignmentQCTaskUploader(ucsc_storage_client_path=self.ucsc_storage_client_path, ucsc_storage_host=self.ucsc_storage_host, filename=bamFile, uuid=self.fileToUUID(bamFile, analysis["bundle_uuid"]), bundle_uuid=analysis["bundle_uuid"]))

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
        return luigi.LocalTarget('/tmp/AlignmentQCTask-%s.txt' % ts_str)

    def fileToUUID(self, input, bundle_uuid):
        # FIXME: this is hard coded, need to do a lookup in the future
        return "afb54dff-41ad-50e5-9c66-8671c53a278b"

if __name__ == '__main__':
    luigi.run()
