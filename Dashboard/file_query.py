#queries using Elasticsearch using python

from elasticsearch import Elasticsearch
import json
import time
import random

es_index_host = 'localhost'
es_index_port = '9200'

# now query elasticsearch
es = Elasticsearch([{'host': es_index_host, 'port': es_index_port}])
# see jqueryflag_alignment_qc
# curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @jqueryflag_alignment_qc
res = es.search(index="analysis_index", body={"query":{"match_all":{}}}, size=5000)

out = open("elasticsearch.jsonl", "w")

listOfFiles = []
i = 0
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
    print("%(donor_uuid)s %(center_name)s %(project)s" % hit["_source"])
    for specimen in hit["_source"]["specimen"]:
        for sample in specimen["samples"]:
            for analysis in sample["analysis"]:
                print "ANALYSIS: "+analysis["analysis_type"]+" "+str(hit["_source"]["flags"]["normal_alignment_qc_report"])+" "+specimen["submitter_specimen_type"]
                for file in analysis["workflow_outputs"]:
                    file_hash = {
                        "id" : str(i),
                        "title" : file["file_path"],
                        "file_type" : file["file_type"],
                        "workflow" : analysis["workflow_name"]+" "+analysis["workflow_version"],
                        "specimen_type": specimen["submitter_specimen_type"],
                        "analysis_type": analysis["analysis_type"],
                        "center_name" : hit["_source"]["center_name"],
                        "project" : hit["_source"]["project"],
                        "program" : hit["_source"]["program"]
                    }
                    out.write(json.dumps({"index":{"_id": str(i),"_type":"meta"}}))
                    out.write("\n")
                    out.write(json.dumps(file_hash))
                    out.write("\n")
                    i += 1
out.close()
