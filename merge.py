from pprint import pprint
from jsonmerge import merge
import json

files = ['sample_individual_metadata_bundle_jsons/','1a_donor_biospecimen.json','1b_donor_biospecimen.json','2a_fastq_upload.json','2b_fastq_upload.json','3a_alignment.json','3b_alignment.json','4_variant_calling.json']
data = []

for i in range(7):
   with open(files[0]+files[i+1]) as data_file:
      data.append(json.load(data_file))

result = merge(data[0], data[1])

specimen_type = ['normal_specimen','tumor_specimen'] 
type = ['samples','sample_uuid','sequence_upload','alignment']

# finds the uuid in 2a, 2b, 3a, 3b and then adds data to the correct branch
# j controls the type (normal or tumor). k and i place the data in the correct place
i=0 
k=0
for j in range(2,6):
   workflows={}
   for uuid in data[j]['parent_uuids']:
      print ("UUIDs: "+uuid)
      workflows[uuid] = data[j]
   specimens = result[specimen_type[j%2]]
   for specimen in specimens:
      samples = specimen[type[0]]
      for sample in samples:
         sample_uuid = sample[type[1]]
         print(sample_uuid)
         sample[type[2+k]] = workflows[sample_uuid]
         pprint(sample)
   i=i+1
   if (i%2==0):
      k=1
#4
workflows={}
for uuid in data[6]['parent_uuids']:
   print ("UUIDs: "+uuid)
   workflows[uuid] = data[6]

donor_uuid = result['donor_uuid']
result['somatic_variant_calling'] = workflows[donor_uuid]

with open('merge.json', 'w') as outfile:
   json.dump(result, outfile)

pprint(result)
