#   Authors: Jean Rodriguez & Chris Wong
#   Date: July 2016
#
#   Description:This script merges metadata json files into one jsonl file. Each json object is grouped by donor and then each individual
#   Donor object is merged into one jsonl file.
#
#   Usage: python merge_gen_meta.py --directory output_metadata_7_20/ --metadataSchema metadata_schema.json

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
    #parser.add_argument('-r', '--skip_Project_Test', help='Lets user skip certain json files that contain a specific program test')
    #parser.add_argument('-t', '--only_Project_Test', help='Lets user include certain json files that contain a specific program  test')

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
        if program != only_program_option:
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

    de_timestamp =  dateutil.parser.parse(detachedObjs["timestamp"])
    for parent_uuid in detachedObjs["parent_uuids"]:
        for key in uuid_mapping:
            donor_obj= uuid_mapping[key]
            donor_timestamp=  dateutil.parser.parse(donor_obj["timestamp"])
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
                                    if timestamp_diff.total_seconds() < 0:
                                        sample["analysis"].remove(analysisObj)
                                        sample["analysis"].append(detachedObjs)

            timestamp_diff = donor_timestamp - de_timestamp
            if timestamp_diff.total_seconds() < 0:
                donor_obj["timestamp"] = detachedObjs["timestamp"]


def mergeDonors(metadataObjs):
    '''
    merge data bundle metadata.json objects into correct donor objects
    '''
    donorMapping = {}
    uuid_to_timestamp={}

    for metaObj in metadataObjs:
        # check if donor exists
        donor_uuid = metaObj["donor_uuid"]

        if not donor_uuid in donorMapping:
            donorMapping[donor_uuid] = metaObj
            uuid_to_timestamp[donor_uuid]= [metaObj["timestamp"]]
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

                        # timestamp mapping
                        if "timestamp" in bundle:
                            uuid_to_timestamp[donor_uuid].append(bundle["timestamp"])
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
                            # timestamp mapping
                            if "timestamp" in bundle:
                                uuid_to_timestamp[donor_uuid].append(bundle["timestamp"])

                        if semver.compare(saved_version, new_workflow_version) == 0:
                            # use the timestamp to determine which analysis to choose
                            if "timestamp" in bundle and "timestamp" in analysisObj :
                                saved_timestamp = dateutil.parser.parse(analysisObj["timestamp"])
                                new_timestamp= dateutil.parser.parse(bundle["timestamp"])
                                timestamp_diff = saved_timestamp - new_timestamp

                                if timestamp_diff.total_seconds() < 0:
                                    sampleObj["analysis"].remove(analysisObj)
                                    sampleObj["analysis"].append(bundle)
                                    # timestamp mapping
                                    if "timestamp" in bundle:
                                        uuid_to_timestamp[donor_uuid].append(bundle["timestamp"])

    # Get the  most recent timstamp from uuid_to_timestamp(for each donor) and use donorMapping to substitute it
    for uuid in uuid_to_timestamp:
        timestamp_list= uuid_to_timestamp[uuid]
        donorMapping[uuid]["timestamp"] = max(timestamp_list)

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



def allHaveItems(lenght):
    result= False
    if lenght == 0:
        result =True

    return result


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


def createFlags(uuid_to_donor):
    for uuid in uuid_to_donor:
        json_object = uuid_to_donor[uuid]
        flagsWithArrs = {'normal_sequence': arrayMissingItems('sequence_upload', "^Normal - ", json_object),
                         'tumor_sequence': arrayMissingItems('sequence_upload',
                                                             "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -",
                                                             json_object),
                         'normal_alignment': arrayMissingItems('alignment', "^Normal - ", json_object),
                         'normal_alignment_qc_report': arrayMissingItems('alignment_qc_report', "^Normal - ", json_object),
                         'tumor_alignment': arrayMissingItems('alignment',
                                                              "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -",
                                                              json_object),
                         'tumor_alignment_qc_report': arrayMissingItems('alignment_qc_report',
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

        normal_sequence= len(flagsWithArrs["normal_sequence"])
        normal_alignment= len(flagsWithArrs["normal_alignment"])
        normal_alignment_qc_report= len(flagsWithArrs["normal_alignment_qc_report"])
        normal_rnaseq_variants= len(flagsWithArrs["normal_rnaseq_variants"])
        normal_germline_variants= len(flagsWithArrs["normal_germline_variants"])

        tumor_sequence= len(flagsWithArrs["tumor_sequence"])
        tumor_alignment= len(flagsWithArrs["tumor_alignment"])
        tumor_alignment_qc_report= len(flagsWithArrs["tumor_alignment_qc_report"])
        tumor_rnaseq_variants= len(flagsWithArrs["tumor_rnaseq_variants"])
        tumor_somatic_variants= len(flagsWithArrs["tumor_somatic_variants"])

        flagsWithStr = {'normal_sequence' :allHaveItems(normal_sequence),
                        'tumor_sequence': allHaveItems(tumor_sequence),
                        'normal_alignment': allHaveItems(normal_alignment),
                        'normal_alignment_qc_report': allHaveItems(normal_alignment_qc_report),
                        'tumor_alignment': allHaveItems(tumor_alignment),
                        'tumor_alignment_qc_report': allHaveItems(tumor_alignment_qc_report),
                        'normal_rnaseq_variants': allHaveItems(normal_rnaseq_variants),
                        'tumor_rnaseq_variants': allHaveItems(tumor_rnaseq_variants),
                        'normal_germline_variants': allHaveItems(normal_germline_variants),
                        'tumor_somatic_variants': allHaveItems(tumor_somatic_variants)}



        normal_sum= normal_germline_variants + normal_rnaseq_variants + normal_alignment + normal_sequence
        if normal_sum==0:
            flagsWithStr["normal_sequence"]= False
            flagsWithStr["normal_alignment"]= False
            flagsWithStr["normal_rnaseq_variants"]= False
            flagsWithStr["normal_germline_variants"]= False

        tumor_sum= tumor_somatic_variants + tumor_rnaseq_variants + tumor_alignment + tumor_sequence
        if tumor_sum==0:
            flagsWithStr["tumor_sequence"]= False
            flagsWithStr["tumor_alignment"]= False
            flagsWithStr["tumor_rnaseq_variants"]= False
            flagsWithStr["tumor_somatic_variants"]= False


        json_object['flags'] = flagsWithStr
        json_object['missing_items'] = flagsWithArrs


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

    # Loads the json files and stores them into an array.
    load_json_arr(data_input, data_arr)

    donorLevelObjs = []
    detachedObjs = []

    # Separates the detached anlaysis obj from the donor obj
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

    # Inserts the detached analysis to the merged donor obj
    uuid_mapping = mergeDonors(donorLevelObjs)
    for de_obj in detachedObjs:
        insert_detached_metadata(de_obj, uuid_mapping)

    # Creates and adds the flags and missingItems to each donor obj
    createFlags(uuid_mapping)

    # Validates each donor obj
    (validated, invalid) = validate_Donor(uuid_mapping,schema)

    # Creates the jsonl files
    dumpResult(validated, "validated.jsonl")
    dumpResult(invalid, "invalid.jsonl")
    dumpResult(validated, 'elasticsearch.jsonl')


if __name__ == "__main__":
    main()
