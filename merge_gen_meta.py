#   Authors: Jean Rodriguez & Chris Wong
#   Date: July 2016
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
    parser.add_argument('-s', '--skip_Program_Test', help='Lets user skip certain json files that contain a specific program test')
    parser.add_argument('-o', '--only_Program_Test', help='Lets user include certain json files that contain a specific program  test')

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


def load_json_arr(input_dir, data_arr):
    """
    :param input_dir: Directory that contains the json files
    :param data_arr: Empty array

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
                    data_arr.append(json_obj)
                    

def skip_Prog_Test(donorLevelObjs, option):
    for json_obj in donorLevelObjs:
        program = json_obj["program"]
        if program == option:
            donorLevelObjs.remove(json_obj)
            
            
def only_prog_option(donorLevelObjs,only_program_option):
    for json_obj in donorLevelObjs:
        program = json_obj["program"]
        if program != option:
            donorLevelObjs.remove(json_obj)
            
            
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
    for parent_uuid in detachedObjs["parent_uuids"]:
        for key in uuid_mapping:
            donor_obj= uuid_mapping[key]
            donor_uuid = donor_obj["donor_uuid"]
            # Check if it needs to be inserted in the donor section
            if parent_uuid== donor_uuid:
                if "analysis" in donor_obj:
                    donor_obj["analysis"].append(detachedObjs)
                else:
                    donor_obj["analysis"]= [detachedObjs]
            # Check if it needs to be inserted in the specimen section
            for specimen in donor_obj["specimen"]:
                specimen_uuid =specimen["specimen_uuid"]
                if specimen_uuid == parent_uuid:
                    if "analysis" in specimen:
                        specimen["analysis"].append(detachedObjs)
                    else:
                        specimen["analysis"]= [detachedObjs]
                # Check if it needs to be inserted in the sample section
                for sample in specimen["samples"]:
                    sample_uuid= sample["sample_uuid"]
                    if sample_uuid == parent_uuid:

                        analysis_type = detachedObjs["analysis_type"]
                        savedAnalysisTypes = set()
                        
                        for donor_analysis in sample["analysis"]:
                            savedAnalysisType = donor_analysis["analysis_type"]
                            savedAnalysisTypes.add(savedAnalysisType)
                            if analysis_type == savedAnalysisType:
                                analysisObj = donor_analysis
                        
                        if not analysis_type in savedAnalysisTypes:
                            sample["analysis"].append(detachedObjs)
                            continue
                        else:
                            # compare 2 analysis to keep only most relevant one
                            # saved is analysisObj
                            # currently being considered is new_analysis
                            new_workflow_version = detachedObjs["workflow_version"]
                        
                            saved_version = analysisObj["workflow_version"]
                            # current is older than new
                            if semver.compare(saved_version, new_workflow_version) == -1:
                                sample["analysis"].remove(analysisObj)
                                sample["analysis"].append(detachedObjs)
                            if semver.compare(saved_version, new_workflow_version) == 0:
                                # use the timestamp
                                if "timestamp" in detachedObjs and "timestamp" in analysisObj:
                                    saved_timestamp = dateutil.parser.parse(analysisObj["timestamp"])
                                    new_timestamp = dateutil.parser.parse(detachedObjs["timestamp"])
                        
                                    timestamp_diff = saved_timestamp - new_timestamp
                                    if timestamp_diff.total_seconds() > 0:
                                        sample["analysis"].remove(analysisObj)
                                        sample["analysis"].append(detachedObjs)
                            
                            
                        
                        
                                    

def mergeDonors(metadataObjs):
    '''
    merge data bundle metadata.json objects into correct donor objects
    '''
    donorMapping = {}

    for metaObj in metadataObjs:
        # check if donor exists
        donor_uuid = metaObj["donor_uuid"]
        if not donor_uuid in donorMapping:
            donorMapping[donor_uuid] = metaObj
            continue

        # check if specimen exists
        donorObj = donorMapping[donor_uuid]
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
                # print "####__sampleObj__####: ", sampleObj
                if not sample_uuid in savedSampleUuids:
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
                        sampleObj["analysis"].append(bundle)
                        continue
                    else:
                        # compare 2 analysis to keep only most relevant one
                        # saved is analysisObj
                        # currently being considered is bundle
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
    analysis_type = False
    results = []
    for specimen in items['specimen']:
        if re.search(regex, specimen['submitter_specimen_type']):
            for sample in specimen['samples']:
                for analysis in sample['analysis']:
                    
                    if analysis["analysis_type"] == itemsName:
                        analysis_type = True
                        break
                
                if not analysis_type:
                    results.append(sample['sample_uuid'])
                    
                analysis_type = False
               
    return results

def create_missing_items(uuid_to_donor):
    for uuid in uuid_to_donor:
        json_object = uuid_to_donor[uuid]
        flagsWithArrs = {'normal_sequence': arrayMissingItems('sequence_upload', "^Normal - ", json_object),
                         'tumor_sequence': arrayMissingItems('sequence_upload',
                                                             "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -",
                                                             json_object),
                         'normal_alignment': arrayMissingItems('alignment', "^Normal - ", json_object),
                         'tumor_alignment': arrayMissingItems('alignment',
                                                              "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -",
                                                              json_object),
                         'normal_rnaseq_variants': arrayMissingItems('rna_seq_quantification', "^Normal - ", json_object),
                         'tumor_rnaseq_variants': arrayMissingItems('rna_seq_quantification',
                                                                    "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -",
                                                                    json_object),
                         'normal_germline_variants': arrayMissingItems('germline_variant_calling', "^Normal - ", json_object),
                         'tumor_somatic_variants': arrayMissingItems('somatic_variant_calling',
                                                                     "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -",
                                                                     json_object)}


        json_object['missing_items'] = flagsWithArrs
    

def createFlags(uuid_to_donor):
    for uuid in uuid_to_donor:
        result= uuid_to_donor[uuid]
        
        flagsWithStr = {'normal_sequence' : allHaveItems('sequence_upload', "^Normal - ", result),
                        'tumor_sequence': allHaveItems('sequence_upload', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result),
                        'normal_alignment': allHaveItems('alignment', "^Normal - ", result),
                        'tumor_alignment': allHaveItems('alignment', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result),
                        'normal_rnaseq_variants': allHaveItems('rna_seq_quantification', "^Normal - ", result),
                        'tumor_rnaseq_variants': allHaveItems('rna_seq_quantification', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result),
                        'normal_germline_variants': allHaveItems('germline_variant_calling', "^Normal - ", result),
                        'tumor_somatic_variants': allHaveItems('somatic_variant_calling', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)}
        flagsWithArrs = {'normal_sequence' : arrayMissingItems('sequence_upload', "^Normal - ", result) ,
                         'tumor_sequence' : arrayMissingItems('sequence_upload', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result) ,
                         'normal_alignment': arrayMissingItems('alignment', "^Normal - ", result),
                         'tumor_alignment': arrayMissingItems('alignment', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result),
                         'normal_rnaseq_variants': arrayMissingItems('rna_seq_quantification', "^Normal - ", result),
                         'tumor_rnaseq_variants': arrayMissingItems('rna_seq_quantification', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result),
                         'normal_germline_variants': arrayMissingItems('germline_variant_calling', "^Normal - ", result),
                         'tumor_somatic_variants': arrayMissingItems('somatic_variant_calling', "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", result)}
        result['flags'] = flagsWithStr
        result['missing_items'] = flagsWithArrs
        
   
    
def dumpResult(result, filename, ES_file_name="elasticsearch.jsonl"):
    global index_index
    for donor in result:
        if filename not in first_write:
            with open(filename, 'w') as outfile:
                if filename == ES_file_name:
                    outfile.write('{"index":{"_id":"' + str(index_index) + '","_type":"meta"}}\n')
                    index_index += 1
                json.dump(donor, outfile)
                outfile.write('\n')
            first_write[filename] = "true"
        else:
            with open(filename, 'a') as outfile:
                if filename == ES_file_name:
                    outfile.write('{"index":{"_id":"' + str(index_index) + '","_type":"meta"}}\n')
                    index_index += 1
                json.dump(donor, outfile)
                outfile.write('\n')


def main():
    args = input_Options()
    data_input = args.directory
    schema = load_json_obj(args.metadataSchema)
    data_arr = []
    load_json_arr(data_input, data_arr)

    donorLevelObjs = []
    detachedObjs = [] 
    for metaobj in data_arr:
        if "donor_uuid" in metaobj:
            donorLevelObjs.append(metaobj)
        elif "parent_uuids" in metaobj:
            detachedObjs.append(metaobj)
            
    # Skip Program Test Option
    skip_prog_option= args.skip_Program_Test
    if skip_prog_option:
        skip_Prog_Test(donorLevelObjs, skip_prog_option)
        
    # Use Only Program Test Option
    only_program_option= args.only_Program_Test
    if only_program_option:
        only_prog_option(donorLevelObjs,only_program_option)

    uuid_mapping = mergeDonors(donorLevelObjs)
    for de_obj in detachedObjs:
        insert_detached_metadata(de_obj, uuid_mapping)
    
    create_missing_items(uuid_mapping)
    
    
    (validated, invalid) = validate_Donor(uuid_mapping,schema)

    dumpResult(validated, "validated.jsonl")
    dumpResult(invalid, "invalid.jsonl")
    dumpResult(validated, 'elasticsearch.jsonl')


if __name__ == "__main__":
    main()

