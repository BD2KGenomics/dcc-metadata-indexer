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

# now add alignment
with open('sample_individual_metadata_bundle_jsons/3a_alignment.json') as data_file5:
    data5 = json.load(data_file5)
with open('sample_individual_metadata_bundle_jsons/3b_alignment.json') as data_file6:
    data6 = json.load(data_file6)

workflows={}
for uuid in data5['parent_uuids']:
    print ("UUIDs: "+uuid)
    workflows[uuid] = data5
for uuid in data6['parent_uuids']:
    print ("UUIDs: "+uuid)
    workflows[uuid] = data6

normal_specimens = result['normal_specimen']
for specimen in normal_specimens:
    normal_samples = specimen['samples']
    for sample in normal_samples:
        sample_uuid = sample['sample_uuid']
        print(sample_uuid)
        sample['alignment'] = workflows[sample_uuid]
        pprint(sample)

tumor_specimens = result['tumor_specimen']
for specimen in tumor_specimens:
    tumor_samples = specimen['samples']
    for sample in tumor_samples:
        sample_uuid = sample['sample_uuid']
        print(sample_uuid)
        sample['alignment'] = workflows[sample_uuid]
        pprint(sample)

# now for somatic calling
with open('sample_individual_metadata_bundle_jsons/4_variant_calling.json') as data_file7:
    data7 = json.load(data_file7)

workflows={}
for uuid in data7['parent_uuids']:
    print ("UUIDs: "+uuid)
    workflows[uuid] = data7

donor_uuid = result['donor_uuid']
result['somatic_variant_calling'] = workflows[donor_uuid]

with open('merge.json', 'w') as outfile:
    json.dump(result, outfile)

pprint(result)
