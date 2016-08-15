#queries using Elasticsearch using python

from elasticsearch import Elasticsearch
import json
import time
import random
from urllib import urlopen


es_index_host = 'localhost'
es_index_port = '9200'
ucsc_storage_host = 'https://storage2.ucsc-cgl.org'

# now query elasticsearch
es = Elasticsearch([{'host': es_index_host, 'port': es_index_port}])
# see jqueryflag_alignment_qc
# curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @jqueryflag_alignment_qc
res = es.search(index="analysis_index", body={"query":{"match_all":{}}}, size=5000)

out = open("elasticsearch.jsonl", "w")

listOfFiles = []
bundle_uuid_filename_to_file_uuid = {}

# now query the metadata service so I have the mapping of bundle_uuid & file names -> file_uuid
json_str = urlopen(str(ucsc_storage_host+":8444/entities?page=0")).read()
metadata_struct = json.loads(json_str)
print "** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"])
for i in range(0, metadata_struct["totalPages"]):
    print "** CURRENT METADATA TOTAL PAGES: "+str(i)
    json_str = urlopen(str(ucsc_storage_host+":8444/entities?page="+str(i))).read()
    metadata_struct = json.loads(json_str)
    for file_hash in metadata_struct["content"]:
        bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]

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
                        "program" : hit["_source"]["program"],
                        "donor" : hit["_source"]["submitter_donor_id"],
                        "download_id" : bundle_uuid_filename_to_file_uuid[analysis["bundle_uuid"]+"_"+file["file_path"]]
                    }
                    out.write(json.dumps({"index":{"_id": str(i),"_type":"meta"}}))
                    out.write("\n")
                    out.write(json.dumps(file_hash))
                    out.write("\n")
                    i += 1
out.close()
