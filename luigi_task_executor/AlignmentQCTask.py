import luigi
import json
import time
import re
from datetime import datetime
from elasticsearch import Elasticsearch

# TODO:
# * need a tool that takes bundle ID and file path and converts to file_id for downloading by Dockstore CLI
# * I can fire off a Dockstore CLI to write the data back but who makes a metadata.json?  Should I add this to Dockstore CLI?  Add it here?
#   Probably, for now I can do everything in this task (maybe distinct tasks for download, upload, tool run)
#   and then, over time, push this out to Dockstore CLI.
#   So, effectively, I use Luigi really as a workflow but, ultimately, this will need to move out to worker VMs via Consonance and be done via
#   Dockstore CLI (or Toil) in order to gain flexibility.
# RUNNING
# $> rm /tmp/foo-*; PYTHONPATH='' luigi --module AlignmentQCTask AlignmentQCCoordinator --es-index-host localhost --es-index-port 9200

class AlignmentQCTaskWorker(luigi.Task):
    param = luigi.Parameter(default=42)

    def run(self):
        f = self.output().open('w')
        print >>f, "hi there"
        f.close()
        time.sleep(20)

    def output(self):
        return luigi.LocalTarget('/tmp/foo-worker-%s.tzt' % self.param)

class AlignmentQCCoordinator(luigi.Task):
    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')

    def requires(self):
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
                            listOfJobs.append(AlignmentQCTaskWorker(param=specimen["submitter_specimen_type"]))
                            # LEFT OFF WITH: traverse the structure, pull out info to parameterize bamstats!
        return listOfJobs

    def run(self):
        f = self.output().open('w')
        print >>f, "hi there"
        f.close()

    def output(self):
        return luigi.LocalTarget('/tmp/foo-%s.tzt' % self.es_index_port)

if __name__ == '__main__':
    luigi.run()
