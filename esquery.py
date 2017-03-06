#queries using Elasticsearch using python

from elasticsearch import Elasticsearch
import json

es_host = 'localhost:9200'
es_type = "meta"
es = Elasticsearch([es_host])

es_name_query = [
   "All normal and tumor fastq exist.",
   "Fastq normal and tumor exist, no alignment.",
   "Alignment normal and tumor exist, no somatic.",
   "All flags are true. All documents exist."
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
                                    "flags.all_normal_sequence_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_tumor_sequences_exists_flag": [
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
                                    "flags.all_normal_sequence_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_tumor_sequences_exists_flag": [
                                       'true'
                                    ]
                                 }
                              }
                           ],
                           "must_not": [
                              {
                                 "terms": {
                                    "flags.all_normal_alignment_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_tumor_alignment_exists_flag": [
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
                                    "flags.all_normal_alignment_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_tumor_alignment_exists_flag": [
                                       'true'
                                    ]
                                 }
                              }
                           ],
                           "must_not": [
                              {
                                 "terms": {
                                    "flags.all_tumor_somatic_variants_exists_flag": [
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
                                    "flags.all_normal_sequence_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_tumor_sequences_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_normal_alignment_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_tumor_alignment_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_normal_germline_variants_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_tumor_somatic_variants_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_normal_rnaseq_variants_exists_flag": [
                                       'true'
                                    ]
                                 }
                              },
                              {
                                 "terms": {
                                    "flags.all_tumor_rnaseq_variants_exists_flag": [
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
]

#checking if the word represents a number
def repNum(s):
    try: 
        float(s)
        return True
    except ValueError:
        return False

#sample json_docs
# json_docs = []
# with open('merge.json') as f:
#    for line in f:
#       newline = []
#       words = line.split()
#       for word in words:
#          i = 1
#          if ((word[:i]==":" or word[:-i]=="," or word[:-i]=="]" or word[:-i]==":")==False):
#             i+=1
#          if (repNum(word[:-(i+1)])==False):
#             #NOTE: replacing all periods (except in num) with 3 underscores to work with ElasticSearch
#             #losing whitespace in strings
#             word = word.replace(".","___")
#          newline.append(word)
#       #adding document to array to be loaded into Elasticsearch
#       json_docs.append(json.loads(''.join(newline)))
#
#
# #loading above json_docs
# for i in json_docs:
#    res = es.index("es-index", es_type, i)
#    es.indices.refresh(index="es-index")

#checking the number of documents
res = es.search(index="analysis_index", body={"query": {"match_all": {}}})
print("For search for everything, got %d hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
   print("CENTER: %(center_name)s PROGRAM: %(program)s PROJECT: %(project)s DONOR ID: %(submitter_donor_id)s" % hit["_source"])

print "\n"
with open("data.json", 'a') as outfile:
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