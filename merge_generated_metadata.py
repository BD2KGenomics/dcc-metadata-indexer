from pprint import pprint
from jsonmerge import merge
from jsonspec.validators import load  # for validation
import json
import random
import sys
from os import listdir
from os.path import isfile, join
import pprint
import re

try:
    FileNotFoundError
except NameError:
    #py2
    FileNotFoundError = IOError

first_write = dict()
index_index = 0

# TODO:
# * assumes the biospecimen is always the last in the list, this is fragile!!!
# * assignBranch makes assumptions about the ordering of files... can't do that
# * we should design a new version of this tool that actually assumes any document can have biospecimen info in it and correctly merge as needed (it's tough to do this)
# * the flags don't support multiple tumors, it assumes a normal and tumor
# * this code doesn't evaluate multiple versions of each of the analysis types e.g. it doesn't replace older results with newer results for the same workflow, just does the last one to be parsed.  This is wrong.
# * eventually the code will need to apply rules about re-running if the inputs are incomplete, e.g. variant calling re-done if all BAMs weren't used for input
# * need a mode to pull all the metadata from the Storage Service and not local files
# * should produce an updated TSV that contains the assigned UUIDs and upload UUIDs

# Note: the files must be in this particular order:
# folderName, donor, donor, fastqNormal, fastqTumor, alignmentNormal, alignmentTumor, variantCalling

def openFiles(files, data, flags):
    #for i in range(len(files) - 1):
    for i in range(len(files)):
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
    # FIXME: Chris has a single specimen array, Emily has two, one for tumor, one for normal, need to harmonize this
    #specimen_type = ['normal_specimen', 'tumor_specimen']
    specimen_type = ['specimen']

    # FIXME: this assumes the last one is always the biospecimen structure!  That's not necessarily true!
    for j in range(len(data)-1):
        #if (flags[j] == "true"):
        workflows = {}
        for uuid in data[j]['parent_uuids']:
            # print ("UUIDs: "+uuid)
            workflows[uuid] = data[j]
            # now look for a match with specimens
            if result['donor_uuid'] == uuid:
                result[data[j]['analysis_type']] = data[j]
            else:
                for specimen_type_str in specimen_type:
                    for specimen in result[specimen_type_str]:
                        if specimen['specimen_uuid'] == uuid:
                            specimen[data[j]['analysis_type']] = data[j]
                        # now look for a match with samples
                        else:
                            for sample in specimen['samples']:
                                if sample['sample_uuid'] == uuid:
                                    sample[data[j]['analysis_type']] = data[j]

def dumpResult(result, filename):
    global index_index
    if filename not in first_write :
        with open(filename, 'w') as outfile:
            if filename == "elasticsearch.jsonl":
                outfile.write('{"index":{"_id":"'+str(index_index)+'","_type":"meta"}}\n')
            json.dump(result, outfile)
            outfile.write('\n')
        first_write[filename] = "true"
    else:
        with open(filename, 'a') as outfile:
            if filename == "elasticsearch.jsonl":
                outfile.write('{"index":{"_id":"' +str(index_index)+ '","_type":"meta"}}\n')
            json.dump(result, outfile)
            outfile.write('\n')
    index_index += 1


def allHaveItems(itemsName, regex, items):
    total_samples = 0
    matching_samples = 0
    for specimen in items['specimen']:
        if re.search(regex, specimen['submitter_specimen_type']):
            for sample in specimen['samples']:
                total_samples += 1
                if itemsName in sample:
                    matching_samples += 1
    return(total_samples == matching_samples)

def arrayMissingItems(itemsName, regex, items):
    results = []
    for specimen in items['specimen']:
        if re.search(regex, specimen['submitter_specimen_type']):
            for sample in specimen['samples']:
                if itemsName not in sample:
                    results.append(sample['submitter_sample_id'])
    return(results)

def createFlags(result):
    flagsWithStr = [{'all_normal_sequence_exists_flag' : allHaveItems('sequence_upload', "^Normal - ", result)},
                    {'all_tumor_sequences_exists_flag': allHaveItems('sequence_upload', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)},
                    {'all_normal_alignment_exists_flag': allHaveItems('alignment', "^Normal - ", result)},
                    {'all_tumor_alignment_exists_flag': allHaveItems('alignment', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)},
                    {'all_normal_germline_variants_exists_flag': allHaveItems('germline_variant_calling', "^Normal - ", result)},
                    {'all_tumor_somatic_variants_exists_flag': allHaveItems('somatic_variant_calling', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)},
                    {'all_normal_rnaseq_variants_exists_flag': allHaveItems('rna_seq_quantification', "^Normal - ", result)},
                    {'all_tumor_rnaseq_variants_exists_flag': allHaveItems('rna_seq_quantification', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)}]
    flagsWithArrs = [{'normal_sequence_missing_array' : arrayMissingItems('sequence_upload', "^Normal - ", result) },
                    {'tumor_sequences_missing_array' : arrayMissingItems('sequence_upload', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result) },
                    {'normal_alignment_missing_array': arrayMissingItems('alignment', "^Normal - ", result)},
                    {'tumor_alignment_missing_array': arrayMissingItems('alignment', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)},
                    {'normal_germline_variants_missing_array': arrayMissingItems('germline_variant_calling', "^Normal - ", result)},
                    {'tumor_somatic_variants_missing_array': arrayMissingItems('somatic_variant_calling', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)},
                    {'normal_rnaseq_variants_missing_array': arrayMissingItems('rna_seq_quantification', "^Normal - ", result)},
                    {'tumor_rnaseq_variants_missing_array': arrayMissingItems('rna_seq_quantification', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)}]
    result['flags'] = flagsWithStr
    result['missing_items'] = flagsWithArrs

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
    pp.pprint(result)
    validateResult(result)
    createFlags(result)
    dumpResult(result, 'merge.jsonl')
    dumpResult(result, 'elasticsearch.jsonl')
    # pprint(result)


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

# at this point we have a hash keyed on donor uuid containing all the JSON files that are related to this donor
# now we can merge and generate the donor-oriented document
for donor_uuid_key in donor_to_files_hash:
    file_arr = donor_to_files_hash[donor_uuid_key]
    run(file_arr)

