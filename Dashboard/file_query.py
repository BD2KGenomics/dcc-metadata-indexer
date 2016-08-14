#queries using Elasticsearch using python

from elasticsearch import Elasticsearch
import json
import time
import random

es_index_host = 'localhost'
es_index_port = '9200'
es_type = "meta"
es = Elasticsearch([es_host])

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



#checking if the word represents a number
def repNum(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

while True:
    #checking the number of documents
    try:
        res = es.search(index="analysis_index", body={"query": {"match_all": {}}}, size=5000)
        print("For search for everything, got %d hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:
           print("CENTER: %(center_name)s PROGRAM: %(program)s PROJECT: %(project)s DONOR ID: %(submitter_donor_id)s" % hit["_source"])
        print("\n")

        with open("data.json", 'w') as outfile:
           outfile.write('[')
           #querying documents using queries above
           addingcommas = False
           for q_index in range(len(es_queries)):
              if (addingcommas):
                 outfile.write(', ')
              else:
                 addingcommas = True
              response = es.search(index="analysis_index", body=es_queries[q_index])
              #print(json.dumps(response, indent=2))
              count = 0
              program = "NA"
              project = "NA"
              for p in response['aggregations']['project_f']['project'].get('buckets'):
                 count = p.get('doc_count')
                 program = p.get('donor_id').get('buckets')
                 project = p.get('key')

              print(es_name_query[q_index])
              print("count: "+str(count))
              print("program: ", program)
              print("project: ", project)
              print("\n")
              outfile.write('{"Label": "'+es_name_query[q_index]+'", "Count": '+str(count)+'}')
           outfile.write(']')
    except Exception:
        print "Some exception, trying again"

    # sleep random time before next upload
    print "PAUSING..."
    time.sleep(10)
