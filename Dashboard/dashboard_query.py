#queries using Elasticsearch using python

from elasticsearch import Elasticsearch
import json
import time
import random

es_host = 'localhost:9200'
es_type = "meta"
es = Elasticsearch([es_host])

es_name_query = [
   "Genomic alignment BAM are available.",
   "Alignment QC HTML report are available."
]

#sample queries
es_queries = [

   {
      "aggs": {
         "project_f": {
            "aggs": {
               "project": {
                  "terms": {
                     "field": "program",
                     "size": 1000
                  },
                  "aggs": {
                  "donor_id": {
                     "terms": {
                        "field": "project",
                        "size": 10000
                     }
                  }
               }
            }
         },
         "filter": {
            "fquery": {
               "query": {
                  "filtered": {
                     "query": {
                        "bool": {
                           "should": [ {
                              "query_string": {
                              "query": "*"
                           }
                           } ]
                        }
                     },
                     "filter": {
                        "bool": {
                           "must": [
                              {
                                 "terms": {
                                    "flags.normal_alignment": [
                                       'true'
                                    ]
                                 }
                              }
                           ]
                        }
                     }
                  }
               }
            }
         }
      }
   },
      "size": 0
   },

   {
      "aggs": {
         "project_f": {
            "aggs": {
               "project": {
                  "terms": {
                     "field": "program",
                     "size": 1000
                  },
                  "aggs": {
                  "donor_id": {
                     "terms": {
                        "field": "project",
                        "size": 10000
                     }
                  }
               }
            }
         },
         "filter": {
            "fquery": {
               "query": {
                  "filtered": {
                     "query": {
                        "bool": {
                           "should": [ {
                              "query_string": {
                              "query": "*"
                           }
                           } ]
                        }
                     },
                     "filter": {
                        "bool": {
                           "must": [
                              {
                                 "terms": {
                                    "flags.normal_alignment_qc_report": [
                                       'true'
                                    ]
                                 }
                              }
                           ]
                        }
                     }
                  }
               }
            }
         }
      }
   },
      "size": 0
   },

]

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
