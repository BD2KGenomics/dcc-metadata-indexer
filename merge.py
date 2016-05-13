from pprint import pprint
from jsonmerge import merge
import json

with open('sample_individual_metadata_bundle_jsons/1a_donor_biospecimen.json') as data_file1:
    data1 = json.load(data_file1)

with open('sample_individual_metadata_bundle_jsons/1b_donor_biospecimen.json') as data_file2:
    data2 = json.load(data_file2)

result = merge(data1, data2)

pprint(result)

# now start decorating with other documents
# start with the sample Fastq upload

with open('merge.json', 'w') as outfile:
    json.dump(result, outfile)
