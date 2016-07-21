#   Authors: Jean Rodriguez & Chris Wong
#   Date: July
#   Description:This script merges metadata json files into one jsonl file. Each json object is grouped by donor and then each individual
#   Donor object is merged into one jsonl file.
#

import semver
import logging
import os
import os.path
import argparse
import json
import jsonschema
import datetime
import re
import dateutil
import dateutil.parser

first_write = dict()
index_index = 0


def input_Options():
    """
    Creates the parse options
    """
    parser = argparse.ArgumentParser(description='Directory that contains Json files.')
    parser.add_argument('-d', '--directory', help='Directory that contains the json metadata files')
    parser.add_argument('-m', '--metadataSchema', help='File that contains the metadata schema')

    args = parser.parse_args()
    return args


def load_json_obj(json_path):
    """
    :param json_path: Name or path of the json metadata file
    :return: A json object
    """

    json_file = open(json_path, 'r')
    json_obj = json.load(json_file)
    json_file.close()

    return json_obj


def load_json_arr(input_dir, data_arr,schema):
    """
    :param input_dir: Directory that contains the json files
    :param data_arr: Empty array
    :param schema: Schema that the user provides

    Gets all of the json files, converts them into objects and stores
    them in an array.
    """
    for folder in os.listdir(input_dir):
        current_folder = os.path.join(input_dir, folder)
        if os.path.isdir(current_folder):
            for file in os.listdir(current_folder):
                if file.endswith(".json"):
                    current_file = os.path.join(current_folder, file)
                    json_obj = load_json_obj(current_file)
                    if validate_json(json_obj, schema):
                        data_arr.append(json_obj)
                    else:
                        print "Json was not compatible with the schema"


def validate_json(json_obj,schema):
    """
    :param json_obj:
    :return: Returns true if the json is in the correct schema
    """
    try:
        jsonschema.validate(json_obj, schema)
    except Exception as exc:
        logging.error("jsonschema.validate FAILED in validate_json: %s" % (str(exc)))
        return False
    return True


def insert_detached_metadata(detachedObjs, uuid_mapping):
    for de_Obj in detachedObjs:
        parent_uuid= de_Obj["parent_uuid"]
        de_analysis_type = de_Obj["analysis"]["analysis_type"]
        donor_obj= uuid_mapping[parent_uuid]
        for specimen in donor_obj["specimen"]:
            for sample in specimen["samples"]:
                for analysis in sample["analysis"]:

                    # This code is used in the mergeDonor() function
                    # Think about making a function for this part
                    donor_analysis_type= analysis["analysis_type"]
                    savedAnalysisTypes = set()
                    savedAnalysisTypes.add(donor_analysis_type)
                    if donor_analysis_type == donor_analysis_type:
                        analysisObj = analysis

                    if not donor_analysis_type in savedAnalysisTypes:
                        specimen["analysis"].append(analysis)
                        continue
                    else:
                        new_workflow_version = analysis["workflow_version"]
                        new_timestamp = analysis["timestamp"]

                        saved_version = analysisObj["workflow_version"]
                        # current is older than new
                        if semver.compare(saved_version, new_workflow_version) == -1:
                            sample["analysis"].remove(analysisObj)
                            sample["analysis"].append(analysisObj)
                        if semver.compare(saved_version, new_workflow_version) == 0:
                            # use the timestamp
                            if "timestamp" in sample and "timestamp" in analysisObj:
                                saved_timestamp = dateutil.parser.parse(analysisObj["timestamp"])
                                new_timestamp = dateutil.parser.parse(analysis["timestamp"])

                                timestamp_diff = saved_timestamp - new_timestamp
                                if timestamp_diff.total_seconds() > 0:
                                    sample["analysis"].remove(analysisObj)
                                    sample["analysis"].append(analysisObj)
                                    

def mergeDonors(metadataObjs):
    '''
    merge data bundle metadata.json objects into correct donor objects
    '''
    donorMapping = {}

    for metaObj in metadataObjs:
        #if metaObj["parent_uuid"] exists:
            #insert_detached_metadata(metaObj,donorMapping)
        # check if donor exists
        donor_uuid = metaObj["donor_uuid"]
        if not donor_uuid in donorMapping:
            donorMapping[donor_uuid] = metaObj
            continue

        # check if specimen exists
        donorObj = donorMapping[donor_uuid]

        #specimen_uuid = metaObj["specimen"][0]["specimen_uuid"]
        for specimen in metaObj["specimen"]:
            specimen_uuid = specimen["specimen_uuid"]

            savedSpecUuids = set()
            for savedSpecObj in donorObj["specimen"]:
                savedSpecUuid = savedSpecObj["specimen_uuid"]
                savedSpecUuids.add(savedSpecUuid)
                if specimen_uuid == savedSpecUuid:
                    specObj = savedSpecObj

            if not specimen_uuid in savedSpecUuids:
                donorObj["specimen"].append(specimen)
                continue

            # check if sample exists
            for sample in specimen["samples"]:
                sample_uuid = sample["sample_uuid"]

                savedSampleUuids = set()
                for savedSampleObj in specObj["samples"]:
                    savedSampleUuid = savedSampleObj["sample_uuid"]
                    savedSampleUuids.add(savedSampleUuid)
                    if sample_uuid == savedSampleUuid:
                        sampleObj = savedSampleObj

                if not sample_uuid in savedSampleUuids:
                    # sampleObj = metaObj["specimen"][0]["samples"][0]
                    specObj["samples"].append(sample)
                    continue

                # check if analysis exists
                # need to compare analysis for uniqueness by looking at analysis_type... bundle_uuid is not the right one here.
                for bundle in sample["analysis"]:
                    bundle_uuid = bundle["bundle_uuid"]
                    analysis_type = bundle["analysis_type"]
                    savedAnalysisTypes = set()
                    for savedBundle in sampleObj["analysis"]:
                        savedAnalysisType = savedBundle["analysis_type"]
                        savedAnalysisTypes.add(savedAnalysisType)
                        if analysis_type == savedAnalysisType:
                            analysisObj = savedBundle

                    if not analysis_type in savedAnalysisTypes:
                        # analysisObj = metaObj["specimen"][0]["samples"][0]["analysis"][0]
                        sampleObj["analysis"].append(bundle)
                        continue
                    else:
                        # compare 2 analysis to keep only most relevant one
                        # saved is analysisObj
                        # currently being considered is bundle
                        # TODO keep only latest version of analysis, compare versions with semver.compare()
                        # new_workflow_version = metaObj["specimen"][0]["samples"][0]["analysis"][0]["workflow_version"]
                        new_workflow_version= bundle["workflow_version"]
                        
                        saved_version= analysisObj["workflow_version"]
                            # current is older than new
                        if semver.compare(saved_version, new_workflow_version) == -1:
                            sampleObj["analysis"].remove(analysisObj)
                            sampleObj["analysis"].append(bundle)
                        if semver.compare(saved_version, new_workflow_version) == 0:
                            # use the timestamp to determine which analysis to choose
                            
                            if "timestamp" in bundle and "timestamp" in analysisObj :
                                saved_timestamp = dateutil.parser.parse(analysisObj["timestamp"])
                                new_timestamp= dateutil.parser.parse(bundle["timestamp"])

                                timestamp_diff = saved_timestamp - new_timestamp
                                if timestamp_diff.total_seconds() > 0:
                                    sampleObj["analysis"].remove(analysisObj)
                                    sampleObj["analysis"].append(bundle)
                                    
    return donorMapping
        
        
        
def validate_Donor(uuid_mapping, schema):
    valid = []
    invalid = []

    for uuid in uuid_mapping:
        donor_Obj = uuid_mapping[uuid]
        if validate_json(donor_Obj, schema):
            valid.append(donor_Obj)
        else:
            invalid.append(donor_Obj)
    return valid, invalid

def allHaveItems__old(itemsName, regex, items):
    total_samples = 0
    matching_samples = 0
    for specimen in items['specimen']:
        if re.search(regex, specimen['submitter_specimen_type']):
            for sample in specimen['samples']:
                total_samples += 1
                if itemsName in sample:
                    matching_samples += 1
    print regex
    print itemsName
    print "total_samples: ", total_samples
    print "total_samples: ", matching_samples
    
    return(total_samples == 1 and matching_samples == 1)

def allHaveItems(itemsName, regex, items):
    total_samples = 0
    analysis_match = 0
    for specimen in items['specimen']:
        if re.search(regex, specimen['submitter_specimen_type']):
            total_samples += 1
            for sample in specimen['samples']:
                for analysis in sample['analysis']:
                    if analysis["analysis_type"] == itemsName:
                        analysis_match += 1
    if total_samples > 0 and analysis_match > 0:
        return(total_samples == analysis_match)
    else:
        return False

def arrayMissingItems(itemsName, regex, items):
    results = []
    for specimen in items['specimen']:
        if re.search(regex, specimen['submitter_specimen_type']):
            for sample in specimen['samples']:
                for analysis in sample['analysis']:
                    if analysis["analysis_type"] != itemsName:
                        results.append(sample['submitter_sample_id'])
    return(results)

def createFlags(uuid_to_donor):
    for uuid in uuid_to_donor:
        result= uuid_to_donor[uuid]
        print uuid
        
        flagsWithStr = {'normal_sequence' : allHaveItems('sequence_upload', "^Normal - ", result),
                        'tumor_sequence': allHaveItems('sequence_upload', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result),
                        'normal_alignment': allHaveItems('alignment', "^Normal - ", result),
                        'tumor_alignment': allHaveItems('alignment', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result),
                        'normal_germline_variants': allHaveItems('germline_variant_calling', "^Normal - ", result),
                        'tumor_somatic_variants': allHaveItems('somatic_variant_calling', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result),
                        'normal_rnaseq_variants': allHaveItems('rna_seq_quantification', "^Normal - ", result),
                        'tumor_rnaseq_variants': allHaveItems('rna_seq_quantification', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)}
        flagsWithArrs = {'normal_sequence' : arrayMissingItems('sequence_upload', "^Normal - ", result) ,
                        'tumor_sequence' : arrayMissingItems('sequence_upload', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result) ,
                        'normal_alignment': arrayMissingItems('alignment', "^Normal - ", result),
                        'tumor_alignment': arrayMissingItems('alignment', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result),
                        'normal_germline_variants': arrayMissingItems('germline_variant_calling', "^Normal - ", result),
                        'tumor_somatic_variants': arrayMissingItems('somatic_variant_calling', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result),
                        'normal_rnaseq_variants': arrayMissingItems('rna_seq_quantification', "^Normal - ", result),
                        'tumor_rnaseq_variants': arrayMissingItems('rna_seq_quantification', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)}
        result['flags'] = flagsWithStr
        result['missing_items'] = flagsWithArrs
        
   
    
def dumpResult(result, filename, ES_file_name="elasticsearch.jsonl"):
    global index_index
    for donor in result:
        if filename not in first_write:
            with open(filename, 'w') as outfile:
                if filename == ES_file_name:
                    outfile.write('{"index":{"_id":"' + str(index_index) + '","_type":"meta"}}\n')
                json.dump(donor, outfile)
                outfile.write('\n')
            first_write[filename] = "true"
        else:
            with open(filename, 'a') as outfile:
                if filename == ES_file_name:
                    outfile.write('{"index":{"_id":"' + str(index_index) + '","_type":"meta"}}\n')
                json.dump(donor, outfile)
                outfile.write('\n')
    index_index += 1


def main():
    args = input_Options()
    data_input = args.directory
    schema = load_json_obj(args.metadataSchema)
    data_arr = []
    load_json_arr(data_input, data_arr, schema)

    donorLevelObjs = []
    detachedObjs = []
    for metaobj in data_arr:
        if "donor_uuid" in metaobj:
            donorLevelObjs.append(metaobj)
        elif "parent_uuids" in data_arr:
            detachedObjs.append(metaobj)
    print "Detached: ", detachedObjs

    uuid_mapping = mergeDonors(donorLevelObjs)
    insert_detached_metadata(detachedObjs, uuid_mapping)

    createFlags(uuid_mapping)

    (validated, invalid) = validate_Donor(uuid_mapping,schema)

    dumpResult(validated, "validated.jsonl")
    dumpResult(invalid, "invalid.jsonl")


if __name__ == "__main__":
    main()

