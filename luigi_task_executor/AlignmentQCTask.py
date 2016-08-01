import luigi
import json
from datetime import datetime
from elasticsearch import Elasticsearch

# TODO:
# * need a tool that takes bundle ID and file path and converts to file_id for downloading by Dockstore CLI
# * I can fire off a Dockstore CLI to write the data back but who makes a metadata.json?  Should I add this to Dockstore CLI?  Add it here?
#   Probably, for now I can do everything in this task (maybe distinct tasks?) and then, over time, push this out to Dockstore CLI.
#   So, effectively, I use Luigi really as a workflow but, ultimately, this will need to move out to worker VMs via Consonance and be done via
#   Dockstore CLI (or Toil) in order to gain flexibility.

class AlignmentQCTaskWorker(luigi.Task):
    param = luigi.Parameter(default=42)

    def run(self):
        f = self.output().open('w')
        print >>f, "hi there"
        f.close()

    def output(self):
        return luigi.LocalTarget('/tmp/foo-worker-%s.tzt' % self.param)

class AlignmentQCCoordinator(luigi.Task):
    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')

    def requires(self):
        es = Elasticsearch([{'host': self.es_index_host, 'port': self.es_index_port}])
# TODO: customize query to find only suitible donors 
        res = es.search(index="analysis_index", body={"query": {"match_all": {}}})

        print("Got %d Hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:
            print("%(donor_uuid)s %(center_name)s %(project)s" % hit["_source"])
# LEFT OFF WITH: traverse the structure, pull out info to parameterize bamstats!
        return AlignmentQCTaskWorker(param=self.es_index_port)

    def run(self):
        f = self.output().open('w')
        print >>f, "hi there"
        f.close()

    def output(self):
        return luigi.LocalTarget('/tmp/foo-%s.tzt' % self.es_index_port)

if __name__ == '__main__':
    luigi.run()
