from pprint import pprint
from jsonmerge import merge
import json

with open('sample_individual_metadata_bundle_jsons/1a_donor_biospecimen.json') as data_file1:
    data1 = json.load(data_file1)

with open('sample_individual_metadata_bundle_jsons/1b_donor_biospecimen.json') as data_file2:
    data2 = json.load(data_file2)

result = merge(data1, data2)

# now start decorating with other documents
# start with the sample Fastq upload
with open('sample_individual_metadata_bundle_jsons/2a_fastq_upload.json') as data_file3:
    data3 = json.load(data_file3)
with open('sample_individual_metadata_bundle_jsons/2b_fastq_upload.json') as data_file4:
    data4 = json.load(data_file4)

workflows={}

# parse out the UUID of parents
for uuid in data3['parent_uuids']:
    print ("UUIDs: "+uuid)
    workflows[uuid] = data3
for uuid in data4['parent_uuids']:
    print ("UUIDs: "+uuid)
    workflows[uuid] = data4

normal_specimens = result['normal_specimen']
for specimen in normal_specimens:
    normal_samples = specimen['samples']
    for sample in normal_samples:
        sample_uuid = sample['sample_uuid']
        print(sample_uuid)
        sample['sequence_upload'] = workflows[sample_uuid]
        pprint(sample)

tumor_specimens = result['tumor_specimen']
for specimen in tumor_specimens:
    tumor_samples = specimen['samples']
    for sample in tumor_samples:
        sample_uuid = sample['sample_uuid']
        print(sample_uuid)
        sample['sequence_upload'] = workflows[sample_uuid]
        pprint(sample)

with open('merge.json', 'w') as outfile:
    json.dump(result, outfile)

pprint(result)
