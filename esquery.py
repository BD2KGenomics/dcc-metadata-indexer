#queries using Elasticsearch using python

from elasticsearch import Elasticsearch
import json

es_host = 'localhost:9200'
es_type = "meta"
es = Elasticsearch([es_host])

es_name_query = [
   "donor1 and donor2 exist, no fastq",
   "all flags are true. All documents exist.",
   "alignment normal and tumor exist, no somatic",
   "fastq normal and tumor exist, no alignment"
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
                                    "flags.donor1_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.donor2_exists": [
                                       'true'
                                    ]
                                 }
                              }
                           ],
                           "must_not": [
                              {
                                 "terms": {
                                    "flags.fastqNormal_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.fastqtumor_exists": [
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
   #donor1 and donor2 exist
   #How many samples are pending upload (they lack a sequence upload)?

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
                                    "flags.alignmentNormal_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.alignmentTumor_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.fastqNormal_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.fastqTumor_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.donor1_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.donor2_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.variantCalling_exists": [
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
   #all flags are 'true'
   #How many donors are complete in their upload vs. how many have one or more missing samples?

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
                                    "flags.alignmentNormal_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.alignmentTumor_exists": [
                                       'true'
                                    ]
                                 }
                              }
                           ],
                           "must_not": [
                              {
                                 "terms": {
                                    "flags.variantCalling_exists": [
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
   #alignment normal and tumor exist
   #somatic variant calling does not exist
   #How many tumor WES/WGS/panel samples have alignment done but no somatic variant calling done?

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
                                    "flags.fastqNormal_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.fastqTumor_exists": [
                                       'true'
                                    ]
                                 }
                              }
                           ],
                           "must_not": [
                              {
                                 "terms": {
                                    "flags.alignmentNormal_exists": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.alignmentTumor_exists": [
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
   }
   #fastq normal and tumor exist
   #alignment does not exist
   #How many samples have fastq uploaded but don’t have alignment?

   #How many tumor RNAseq samples have alignment done but no expression values done?
]

def repNum(s):
    try: 
        float(s)
        return True
    except ValueError:
        return False

#sample json_docs
json_docs = []
with open('merge.json') as f:
   for line in f:
      newline = []
      words = line.split()
      for word in words:
         i = 1
         if ((word[:i]==":" or word[:-i]=="," or word[:-i]=="]" or word[:-i]==":")==False):
            i+=1
         if (repNum(word[:-(i+1)])==False):
            #NOTE: replacing all periods (except in num) with 3 underscores to work with ElasticSearch 
            #losing whitespace in strings
            word = word.replace(".","___")
         newline.append(word)
      json_docs.append(json.loads(''.join(newline)))
      

#loading above json_docs
for i in json_docs:
   res = es.index("es-index", es_type, i)
   es.indices.refresh(index="es-index")

#checking the number of documents
res = es.search(index="es-index", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
   print("%(center_name)s %(program)s: %(project)s" % hit["_source"])

#querying documents using queries above
for q_index in range(len(es_queries)):
   response = es.search(index="es-index", body=es_queries[q_index])
   #print(json.dumps(response, indent=2))
   for p in response['aggregations']['project_f']['project'].get('buckets'):
      count = p.get('doc_count')
      program = p.get('donor_id').get('buckets')
      project = p.get('key')
   
   print(es_name_query[q_index])
   print("count:",count)
   print("program:",program)
   print("project: "+project+"\n")