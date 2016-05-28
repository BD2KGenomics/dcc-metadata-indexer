from pprint import pprint
from jsonmerge import merge
from jsonspec.validators import load  # for validation
import json
import random
import sys
from os import listdir
from os.path import isfile, join
import pprint

try:
    FileNotFoundError
except NameError:
    #py2
    FileNotFoundError = IOError

# TODO:
# * assumes the biospecimen is always the last in the list, this is fragile!!!
# * assignBranch makes assumptions about the ordering of files... can't do that

# Note: the files must be in this particular order:
# folderName, donor, donor, fastqNormal, fastqTumor, alignmentNormal, alignmentTumor, variantCalling

def openFiles(files, data, flags):
    for i in range(len(files) - 1):
        try:
            with open(files[i]) as data_file:
                data.append(json.load(data_file))
            flags.append("true")
        except FileNotFoundError:
            print('File not found: ' + files[i])
            flags.append("false")
            data.append(0)

# FIXME: This makes assumptions about the ordering of files... can't do that
def assignBranch(data, flags, result):
    # finds the uuid in 2a, 2b, 3a, 3b and then adds data to the correct branch
    # j controls the type (normal or tumor). k and i place the data in the correct place
    specimen_type = ['normal_specimen', 'tumor_specimen']
    type = ['samples', 'sample_uuid', 'sequence_upload', 'alignment']
    i = 0
    k = 0
    for j in range(0, len(data)-1):
        if (flags[j] == "true"):
            workflows = {}
            for uuid in data[j]['parent_uuids']:
                # print ("UUIDs: "+uuid)
                workflows[uuid] = data[j]
            specimens = result[specimen_type[j % 2]]
            for specimen in specimens:
                samples = specimen[type[0]]
                for sample in samples:
                    sample_uuid = sample[type[1]]
                    # print(sample_uuid)
                    sample[type[2 + k]] = workflows[sample_uuid]
                    # pprint(sample)
        i = i + 1
        if (i % 2 == 0):
            k = 1


def assignVariant(data, flags, result):
    if (flags[6] == "true"):
        workflows = {}
        for uuid in data[6]['parent_uuids']:
            # print ("UUIDs: "+uuid)
            workflows[uuid] = data[6]
        donor_uuid = result['donor_uuid']
        result['somatic_variant_calling'] = workflows[donor_uuid]


def dumpResult(result):
    with open('merge.json', 'a') as outfile:
        # newidtype = dict(_id=1, _type='meta') #could use this for elasticsearch bulk queries
        # newindex = dict(index = newidtype)
        # json.dump(newindex, outfile)
        # outfile.write('\n')
        json.dump(result, outfile)
        outfile.write('\n')


def createFlags(flags, result):
    flagsWithStr = dict(zip(
        ['donor1_exists', 'donor2_exists', 'fastqNormal_exists', 'fastqTumor_exists', 'alignmentNormal_exists',
         'alignmentTumor_exists', 'variantCalling_exists'], flags))
    result['flags'] = flagsWithStr


def validateResult(result):
    # data will validate against this schema
    with open('metadata_model_no_uri.json') as data_file:
        schema = json.load(data_file)

    # loading into jsonspec
    validator = load(schema)

    # validate result against schema
    validator.validate(result)


def run(files):
    data = []
    flags = []
    result = {}
    flagsWithStr = {}
    openFiles(files, data, flags)
    #result = merge(data[0], data[1])
    result = data[len(data)-1]
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint (result)
    assignBranch(data, flags, result)
    assignVariant(data, flags, result)
    validateResult(result)
    createFlags(flags, result)
    dumpResult(result)
    # pprint(result)


def fileRandom(files):
    file = ['sample_individual_metadata_bundle_jsons/', '1a_donor_biospecimen.json', '1b_donor_biospecimen.json',
            '2a_fastq_upload.json', '2b_fastq_upload.json', '3a_alignment.json', '3b_alignment.json',
            '4_variant_calling.json']
    files.append(file[0])
    files.append(file[1])
    files.append(file[2])
    for f in range(3, 8):
        x = random.randint(1, 10)
        if (x >= 5):
            files.append(file[f])
        else:
            files.append("no" + file[f])

def find_values(id, json_repr):
    results = []

    def _decode_dict(a_dict):
        try:
            results.append(a_dict[id])
        except KeyError:
            pass
        return a_dict

    json.loads(json_repr, object_hook=_decode_dict)  # return value ignored
    return results


# here's where I need to customize
# 1) need to parse all files and look for specimen/sample to donor mapping
# 2) parse all files again, binning them based on parent UUIDs
# 3) now that things are binned, for each bin, aggregagate the documents based on the parent uuids
# 4) print out the complete donor documents as JSONL (or individual files if that param is given)
pp = pprint.PrettyPrinter(indent=4)
to_donor_id_hash = dict()
donor_to_files_hash = dict()

# this finds all the sample and specimen UUIDs and maps them to donor ID
for index in range(1, len(sys.argv)):
    for f in listdir(sys.argv[index]):
        if isfile(sys.argv[index] + "/" + f):
            print "FILE! "+sys.argv[index] + "/" + f
            json_data = open(sys.argv[index] + "/" + f).read()
            donor_uuid_arr = find_values('donor_uuid', json_data)
            if donor_uuid_arr:
                donor_uuid = donor_uuid_arr[0]
                print "DONOR ID: " + donor_uuid
                specimen_uuids = find_values('specimen_uuid', json_data)
                for uuid in specimen_uuids:
                    to_donor_id_hash[uuid] = donor_uuid
                sample_uuids = find_values('sample_uuid', json_data)
                for uuid in sample_uuids:
                    to_donor_id_hash[uuid] = donor_uuid

# now group all the files by donor uuid
for index in range(1, len(sys.argv)):
    for f in listdir(sys.argv[index]):
        if isfile(sys.argv[index] + "/" + f):
            print "FILE! " + sys.argv[index] + "/" + f
            json_data = open(sys.argv[index] + "/" + f).read()
            parent_uuids = find_values('parent_uuids', json_data)
            if len(parent_uuids):
                for uuid in parent_uuids:
                    pp.pprint ("UUID: " + uuid[0] + " to DONOR ID: " + to_donor_id_hash[uuid[0]])
                    if to_donor_id_hash[uuid[0]] in donor_to_files_hash:
                        # append the new number to the existing array at this slot
                        donor_to_files_hash[to_donor_id_hash[uuid[0]]].append(sys.argv[index] + "/" + f)
                    else:
                        # create a new array in this slot
                        donor_to_files_hash[to_donor_id_hash[uuid[0]]] = [sys.argv[index] + "/" + f]
            else:
                donor_uuids = find_values('donor_uuid', json_data)
                if donor_uuids:
                    donor_uuid = donor_uuids[0]
                    print "DONOR ID FOUND "+ donor_uuid
                    if donor_uuid in donor_to_files_hash:
                        # append the new number to the existing array at this slot
                        donor_to_files_hash[donor_uuid].append(sys.argv[index] + "/" + f)
                    else:
                        # create a new array in this slot
                        donor_to_files_hash[donor_uuid] = [sys.argv[index] + "/" + f]

pp.pprint(donor_to_files_hash)

# now loop over the keys of the donor_to_files_hash and merge the files


# at this point we have a hash keyed on donor uuid containing all the JSON files that are related to this donor
# now we can merge and generate the donor-oriented document

for donor_uuid_key in donor_to_files_hash:
    file_arr = donor_to_files_hash[donor_uuid_key]
    run(file_arr)

#for a in range(10):
#    print(a)
#    files = []
#    fileRandom(files)
#    run(files)
