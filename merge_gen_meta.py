#   Author: Jean Rodriguez
#   Date: July
#   Description:This script merges metadata json files into one jsonl file. Each json object is grouped by donor and then each individual
#   Donor object is merged into one jsonl file.
#

import semver
import logging
import sys
import os
import argparse
import json
import jsonschema


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
    print type(json_file)
    json_obj = json.load(json_file)
    print json_obj
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
            for sample in specimen["sample"]:
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
                        new_timestamp = bundle["timestamp"]

                        saved_version= analysisObj["workflow_version"]
                            # current is older than new
                        if semver.compare(saved_version, new_workflow_version) == -1:
                            sampleObj["analysis"].discard(analysisObj)
                            sampleObj["analysis"].append(analysisObj)
                        if semver.compare(saved_version, new_workflow_version) == 0:
                            # use the timestamp
                            if "timestamp" in bundle and "timestamp" in analysisObj :
                                saved_timestamp = dateutil.parser.parse(analysisObj["timestamp"])
                                new_timestamp= dateutil.parser.parse(bundle["timestamp"])

                                timestamp_diff = saved_timestamp - new_timestamp
                                if timestamp_diff.total_seconds() > 0:
                                    sampleObj["analysis"].discard(analysisObj)
                                    sampleObj["analysis"].append(analysisObj)

    return donorMapping


def generate_flags(donor_uuid_to_obj):
    """
    :param donor_uuid_to_obj: Dictionary that contains the donor_uuid mapped with the json object

    Generates the flags and adds it into each donor json object
    """
    for key in donor_uuid_to_obj:
        flags = {"sequence_upload": False,
                 "alignment": False,
                 "germline_variant_calling": False,
                 "somatic_variant_calling": False,
                 "rna_seq_quantification": False}
        
        specimen_list = donor_uuid_to_obj[key]["specimen"]
        for specimen in specimen_list:
            samples_list= specimen["samples"]
            for samples in samples_list:
                analysis_list= samples["analysis"]
                for analysis in analysis_list:
                    analysis_type= analysis["analysis_type"]
                    flag_list = flags.keys()
                    if analysis_type in flag_list:
                        flags[analysis_type] = True
        donor_uuid_to_obj[key]["flags"]= flags
        
        
def create_json_file(donor_uuid_to_obj,schema):
    """
    :param donor_uuid_to_obj: Dictionary that contains the donor_uuid mapped with the json object
    :param schema:  Schema provided by the user json Object

    Stores all of the json objects into the "merge.jsonl" file
    """
    jsonl_file = open("merge.jsonl", "w")
    for uuid in donor_uuid_to_obj:
        if validate_json(donor_uuid_to_obj[uuid],schema):
            json.dump(donor_uuid_to_obj[uuid], jsonl_file)
            jsonl_file.write("\n")
        else:
            # maybe all of the json Objects that are not valid can be stored in a variable
            # so that it can appear at the end of the execution of the program
            print "json file with UUID:" + str(uuid) + " not valid"

    jsonl_file.close()


def main():
    args = input_Options()
    data_input = args.directory
    schema = load_json_obj(args.metadataSchema)
    data_arr = []
    load_json_arr(data_input, data_arr,schema)
    
    donor_uuid_to_obj = mergeDonors(data_arr)

    generate_flags(donor_uuid_to_obj)
    create_json_file(donor_uuid_to_obj,schema)


if __name__ == "__main__":
    main()

