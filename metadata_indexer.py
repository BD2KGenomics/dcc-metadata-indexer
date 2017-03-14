#   Authors: Jean Rodriguez & Chris Wong
#   Date: July 2016
#
#   Description:This script merges metadata json files into one jsonl file. Each json object is grouped by donor and then each individual
#   donor object is merged into one jsonl file.
#
#   Usage: python metadata_indexer.py --only_Program TEST --only_Project TEST --awsAccessToken `cat ucsc-storage-client/accessToken`  --clientPath ucsc-storage-client/ --metadataSchema metadata_schema.json


import semver
import logging
import os
import os.path
import platform
import argparse
import json
import jsonschema
import datetime
import re
import dateutil
import ssl
import dateutil.parser
import ast
from urllib import urlopen
from subprocess import Popen, PIPE

first_write = dict()
index_index = 0
#Dictionary to hold the File UUIDs to later get the right file size
bundle_uuid_filename_to_file_uuid = {}
#List that will hold the UUIDs and sizes
#file_uuid_and_size = []

#Call the storage endpoint and get the list of the 
def get_size_list(token, redwood_host):
     """
     This function assigns file_uuid_and_size with all the ids and file size, 
     so they can be used later to fill the missing file_size entries
     """
     print "Downloading the listing"
     #c = pycurl.Curl()
     #Attempt to download
     try:
          #Set the curl options
          #c.setopt(c.URL, "https://aws:"+token+"@"+redwood_host+":5431/listing")
          #c.setopt(c.SSL_VERIFYPEER, 0)
          #c.setopt(c.HTTPHEADER, ['Authorization: Bearer '+token])
          #p.setopt(pycurl.WRITEFUNCTION, lambda x: None)
          
          #file_uuid_and_size = c.perform()
          command = ["curl"]
          command.append("-k")
          command.append("-H")
          command.append("Authorization: Bearer "+token)
          command.append("https://aws:"+token+"@"+redwood_host+":5431/listing")
          c_data=Popen(command, stdout=PIPE, stderr=PIPE)
          #c_data=check_call(command, stdout=PIPE, stderr=PIPE)
          size_list, stderr = c_data.communicate()
          #headers = {"Authorization":"Bearer %s" %token}
          #print headers
          #r = requests.get("https://aws:"+token+"@"+redwood_host+":5431/listing", verify=False, headers=headers)
          file_uuid_and_size = ast.literal_eval(size_list) 
          #file_uuid_and_size = r.json()
          print "Done downloading the file size listing"
          #print file_uuid_and_size
     except Exception:
          logging.error('Error while performing the curl operation')
          print 'Error while performing the curl operation'

     print file_uuid_and_size[1]
     return file_uuid_and_size

#Fills in the contents of bundle_uuid_filename_to_file_uuid
def requires(redwood_host):
        """
        Fills the dictionary for the files and their UUIDs. 
        """
        print "** COORDINATOR **"
        print "**ACQUIRING FILE UUIDS**"
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # now query the metadata service so I have the mapping of bundle_uuid & file names -> file_uuid
        print str("https://"+redwood_host+":8444/entities?page=0")
        json_str = urlopen(str("https://"+redwood_host+":8444/entities?page=0"), context=ctx).read()
        metadata_struct = json.loads(json_str)
        print "** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"])
        for i in range(0, metadata_struct["totalPages"]):
             print "** CURRENT METADATA TOTAL PAGES: "+str(i)
             json_str = urlopen(str("https://"+redwood_host+":8444/entities?page="+str(i)), context=ctx).read()
             metadata_struct = json.loads(json_str)
             for file_hash in metadata_struct["content"]:
                  bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]
                  # HACK!!!  Please remove once the behavior has been fixed in the workflow!!
                  if file_hash["fileName"].endswith(".sortedByCoord.md.bam"):
                        bundle_uuid_filename_to_file_uuid[file_hash["gnosId"] + "_sortedByCoord.md.bam"] = file_hash["id"]
                  if file_hash["fileName"].endswith(".tar.gz"):
                        bundle_uuid_filename_to_file_uuid[file_hash["gnosId"] + "_tar.gz"] = file_hash["id"]
                  if file_hash["fileName"].endswith(".wiggle.bg"):
                        bundle_uuid_filename_to_file_uuid[file_hash["gnosId"] + "_wiggle.bg"] = file_hash["id"]

def insert_size(file_name, file_uuid_and_size):
     """
     Opens the file and inserts any missing file_size
     """
     #Open the file and do the size insertion
     with open(file_name, 'r') as f:
          data = json.load(f)
          print file_name
          #print data.items() #TEST
          #Dealing with a different metadata.json format
          if 'workflow_outputs' in data:
               bundle_uuid = data['bundle_uuid']
               for file_ in data['workflow_outputs']:
                    file_name_uploaded = file_['file_path']
                    if 'file_size' not in file_:
                         try:
                              file_uuid = bundle_uuid_filename_to_file_uuid[bundle_uuid+'_'+file_name_uploaded]
                              #print (file_uuid_and_size)
                              #print str(uuid_dict)
                              file_entry = filter(lambda x:x['id'] == file_uuid, file_uuid_and_size)
                              #print str(file_entry) #TEST
                              file_['file_size'] = file_entry[0]['size']
                         except Exception as e:
                              logging.error('Error while assigning missing size. File Id: %s' % file_uuid)
                              print 'Error while assigning missing size. File Id: %s' % file_uuid
                              print str(e)
          
          #The more generic format
          else:
               for specimen in data['specimen']:
                    for sample in specimen['samples']:
                         for analysis in sample['analysis']:
                                   bundle_uuid = analysis['bundle_uuid']
                                   for file_ in analysis['workflow_outputs']:
                                        file_name_uploaded = file_['file_path']
                                        if 'file_size' not in file_:
                                             try:
                                                  #Get the size for the file uuid
                                                  file_uuid = bundle_uuid_filename_to_file_uuid[bundle_uuid+'_'+file_name_uploaded]
                                                  file_entry = filter(lambda x: x['id'] == file_uuid, file_uuid_and_size)
                                                  #print (file_uuid_and_size)
                                                  #print str(uuid_dict)
                                                  #print str(file_entry) #TEST
                                                  file_['file_size'] = file_entry[0]['size']
                                             except Exception as e:
                                                  logging.error('Error while assigning missing size. File Id: %s' % file_uuid)
                                                  print 'Error while assigning missing size. File Id: %s' % file_uuid
                                                  print str(e)
     #Remove and replace the old file with the new one. 
     os.remove(file_name)
     with open(file_name, 'w') as f:
          json.dump(data, f, indent=4)
                         

def input_Options():
    """
    Creates the parse options
    """
    parser = argparse.ArgumentParser(description='Directory that contains Json files.')
    parser.add_argument('-d', '--test-directory', help='Directory that contains the json metadata files')
    parser.add_argument('-u', '--skip-uuid-directory', help='Directory that contains files with file uuids (bundle uuids, one per line, file ending with .redacted) that represent databundles that should be skipped, useful for redacting content (but not deleting it)')
    parser.add_argument('-m', '--metadata-schema', help='File that contains the metadata schema')
    parser.add_argument('-s', '--skip-program', help='Lets user skip certain json files that contain a specific program test')
    parser.add_argument('-o', '--only-program', help='Lets user include certain json files that contain a specific program  test')
    parser.add_argument('-r', '--skip-project', help='Lets user skip certain json files that contain a specific program test')
    parser.add_argument('-t', '--only-project', help='Lets user include certain json files that contain a specific program  test')
    parser.add_argument('-a', '--storage-access-token', default="NA", help='Storage access token to download the metadata.json files')
    parser.add_argument('-c', '--client-path', default="ucsc-storage-client/", help='Path to access the ucsc-storage-client tool')
    parser.add_argument('-n', '--server-host', default="storage.ucsc-cgl.org", help='hostname for the storage service')
    parser.add_argument('-preserve-version',action='store_true', default=False, help='Keep all copies of analysis events')

    args = parser.parse_args()
    return args

def make_output_dir():
    """
    Creates directory named "endpoint_metadata" to store all the metadata that is downloaded
    """
    directory= "endpoint_metadata"
    mkdir_Command=["mkdir"]
    mkdir_Command.append(directory)

    c_data=Popen(mkdir_Command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = c_data.communicate()
    logging.info("created directory: %s/" % (directory))
    print "created directory: %s/" % (directory)
    return directory

def endpint_mapping(data_array):
    """
    data_array: array of json objects
    create a maping: gnos-id -> id
    """
    numberOfElements=0
    page=0
    my_dictionary= dict()
    for j_obj in data_array:
        numberOfElements += j_obj["numberOfElements"]
        page= j_obj["number"]
        for content in j_obj["content"]:
            content_id= content["id"]
            my_dictionary[content_id]={"content": content, "page": page}
    page += 1

    logging.info("Total pages downloaded: %s" % page)
    logging.info("Total number of elements: %s" % numberOfElements)
    print "Total pages downloaded:   ",page
    print "Total number of elements: ", numberOfElements

    return my_dictionary


def create_merge_input_folder(id_to_content,directory,accessToken,client_Path, size_list):
    """
    id_to_content: dictionary that maps content id to content object.
    directory: name of directory where the json files will be stored.

    Uses the ucsc-download.sh script to download the json files
    and store them in the "directory".
    """
    """
    java
    -Djavax.net.ssl.trustStore=/ucsc-storage-client/ssl/cacerts
    -Djavax.net.ssl.trustStorePassword=changeit
    -Dmetadata.url=https://storage.ucsc-cgl.org:8444
    -Dmetadata.ssl.enabled=true -Dclient.ssl.custom=false
    -Dstorage.url=https://storage.ucsc-cgl.org:5431
    -DaccessToken=${accessToken}
    -jar
    /ucsc-storage-client/icgc-storage-client-1.0.14-SNAPSHOT/lib/icgc-storage-client.jar
    download
    --output-dir ${download}
    --object-id ${object}
    --output-layout bundle
    """
    args = input_Options()
    metadataClientJar = os.path.join(client_Path,"icgc-storage-client-1.0.14-SNAPSHOT/lib/icgc-storage-client.jar")
    metadataUrl= "https://"+args.server_host+":8444"
    storageUrl= "https://"+args.server_host+":5431"
    trustStore = os.path.join(client_Path,"ssl/cacerts")
    trustStorePw = "changeit"


    # If the path is not correct then the download and merge will not be performed.
    if not os.path.isfile(metadataClientJar):
        logging.critical("File not found: %s. Path may not be correct: %s" % (metadataClientJar,client_Path))
        print "File not found: %s" % metadataClientJar
        print "Path may not be correct: %s" % client_Path
        print "Exiting program."
        exit(1)

    logging.info('Begin Download.')
    print "downloading metadata..."
    for content_id in id_to_content:
        file_create_time_server = id_to_content[content_id]["content"]["createdTime"]
        if os.path.isfile(directory+"/"+id_to_content[content_id]["content"]["gnosId"]+"/metadata.json") and \
                creation_date(directory+"/"+id_to_content[content_id]["content"]["gnosId"]+"/metadata.json") == file_create_time_server/1000:
            #Assign any missing file size
            insert_size(directory+"/"+id_to_content[content_id]["content"]["gnosId"]+"/metadata.json", size_list)
            #Set the time created to be the one supplied by redwood (since insert_size() modifies the file)
            os.utime(directory + "/" + id_to_content[content_id]["content"]["gnosId"] + "/metadata.json",
                         (file_create_time_server/1000, file_create_time_server/1000))
            #Open the file and add the file size if missing. 
            print "  + using cached file "+directory+"/"+id_to_content[content_id]["content"]["gnosId"]+"/metadata.json created on "+str(file_create_time_server)
            #os.utime(directory + "/" + id_to_content[content_id]["content"]["gnosId"] + "/metadata.json", (file_create_time_server/1000, file_create_time_server/1000))
        else:
            print "  + downloading "+content_id
            # build command string
            command = ["java"]
            command.append("-Djavax.net.ssl.trustStore=" + trustStore)
            command.append("-Djavax.net.ssl.trustStorePassword=" + trustStorePw)
            command.append("-Dmetadata.url=" + str(metadataUrl))
            command.append("-Dmetadata.ssl.enabled=true")
            command.append("-Dclient.ssl.custom=false")
            command.append("-Dstorage.url=" + str(storageUrl))
            command.append("-DaccessToken=" + str(accessToken))
            command.append("-jar")
            command.append(metadataClientJar)
            command.append("download")
            command.append("--output-dir")
            command.append(str(directory))
            command.append("--object-id")
            command.append(str(content_id))
            command.append("--output-layout")
            command.append("bundle")

            #print " ".join(command)

            try:
                c_data=Popen(command, stdout=PIPE, stderr=PIPE)
                stdout, stderr = c_data.communicate()
                # now set the create timestamp
                insert_size(directory+"/"+id_to_content[content_id]["content"]["gnosId"]+"/metadata.json", size_list)
                os.utime(directory + "/" + id_to_content[content_id]["content"]["gnosId"] + "/metadata.json",
                         (file_create_time_server/1000, file_create_time_server/1000))
            except Exception:
                logging.error('Error while downloading file with content ID: %s' % content_id)
                print 'Error while downloading file with content ID: %s' % content_id


    logging.info('End Download.')

def creation_date(path_to_file):
    """
    Try to get the date that a file was created, falling back to when it was
    last modified if that isn't possible.
    See http://stackoverflow.com/a/39501288/1709587 for explanation.
    """
    if platform.system() == 'Windows':
        return os.path.getctime(path_to_file)
    else:
        stat = os.stat(path_to_file)
        try:
            return stat.st_birthtime
        except AttributeError:
            # We're probably on Linux. No easy way to get creation dates here,
            # so we'll settle for when its content was last modified.
            return stat.st_mtime

def load_json_obj(json_path):
    """
    :param json_path: Name or path of the json metadata file.
    :return: A json object.
    """

    json_file = open(json_path, 'r')
    print "JSON FILE: "+json_path
    json_obj = json.load(json_file)
    json_file.close()

    return json_obj


def load_json_arr(input_dir, data_arr, redacted):
    """
    :param input_dir: Directory that contains the json files.
    :param data_arr: Empty array.

    Gets all of the json files, converts them into objects and stores
    them in an array.
    """
    for folder in os.listdir(input_dir):
        current_folder = os.path.join(input_dir, folder)
        if os.path.isdir(current_folder):
            for file in os.listdir(current_folder):
                if file.endswith(".json") and folder not in redacted:
                    current_file = os.path.join(current_folder, file)
                    try:
                        json_obj = load_json_obj(current_file)
                        data_arr.append(json_obj)
                    except ValueError:
                        print "ERROR PARSING JSON: will skip this record."


def skip_option(donorLevelObjs, option_skip, key):
    for json_obj in donorLevelObjs:
        keys = json_obj[key]
        if keys == option_skip:
            donorLevelObjs.remove(json_obj)



def only_option(donorLevelObjs,option_only, key):
    for json_obj in donorLevelObjs:
        keys = json_obj[key]
        if keys != option_only:
            donorLevelObjs.remove(json_obj)


def validate_json(json_obj,schema):
    """
    :return: Returns true if the json is in the correct schema.
    """
    try:
        jsonschema.validate(json_obj, schema)
    except Exception as exc:
        logging.error("jsonschema.validate FAILED in validate_json: %s" % (str(exc)))
        return False
    return True


def insert_detached_metadata(detachedObjs, uuid_mapping, preserve_version=False):
    """
    Inserts a Analysis object, that contains a parent ID, to its respective donor object.
    """
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
                            if preserve_version:
                                sample["analysis"].append(detachedObjs)
                            else:
                                new_workflow_version = detachedObjs["workflow_version"]

                                saved_version = analysisObj["workflow_version"]
                                # current is older than new
                                if saved_version == new_workflow_version:
                                    # use the timestamp
                                    if "timestamp" in detachedObjs and "timestamp" in analysisObj:
                                        saved_timestamp = dateutil.parser.parse(analysisObj["timestamp"])
                                        new_timestamp = dateutil.parser.parse(detachedObjs["timestamp"])

                                        timestamp_diff = saved_timestamp - new_timestamp
                                        if timestamp_diff.total_seconds() < 0:
                                            sample["analysis"].remove(analysisObj)
                                            sample["analysis"].append(detachedObjs)
                                elif semver.compare(saved_version, new_workflow_version) == -1:
                                    sample["analysis"].remove(analysisObj)
                                    sample["analysis"].append(detachedObjs)
                                #if semver.compare(saved_version, new_workflow_version) == 0:


            timestamp_diff = donor_timestamp - de_timestamp
            if timestamp_diff.total_seconds() < 0:
                donor_obj["timestamp"] = detachedObjs["timestamp"]


def mergeDonors(metadataObjs, preserve_version):
    '''
    Merge data bundle metadata.json objects into correct donor objects.
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

                    if not analysis_type in savedAnalysisTypes or preserve_version:
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
    """
    Validates each donor object with the schema provided.
    """
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
    """
    Returns the value of each flag, based on the lenght of the array in 'missing_items'.
    """
    #print ("ALLHAVEITEMS: %s" % lenght)
    result= False
    if lenght == 0:
        result =True
    #print "RESULT: %s" % result
    return result


def arrayMissingItems(itemsName, regex, items,submitter_specimen_types):
    """
    Returns a list of 'sample_uuid' for the analysis that were missing.
    """
    return arrayItems(itemsName, regex, items,submitter_specimen_types, True)

def arrayContainingItems(itemsName, regex, items,submitter_specimen_types):
    """
    Returns a list of 'sample_uuid' for the analysis that were present.
    """
    return arrayItems(itemsName, regex, items,submitter_specimen_types, False)

def arrayItems(itemsName, regex, items,submitter_specimen_types, missing):
    """
    Returns a list of 'sample_uuid' for the analysis that were missing.
    """
    analysis_type = False
    results = []
    for specimen in items['specimen']:
        if re.search(regex, specimen['submitter_specimen_type']):
            submitter_specimen_types.append(specimen['submitter_specimen_type'])
            for sample in specimen['samples']:
                for analysis in sample['analysis']:

                    if analysis["analysis_type"] == itemsName:
                        analysis_type = True
                        break

                if (missing and not analysis_type) or (not missing and analysis_type):
                    results.append(sample['sample_uuid'])

                analysis_type = False

    return results


def arrayMissingItemsWorkflow(workflow_name, workflow_version_regex, regex, items,submitter_specimen_types):
    """
    Returns a list of 'sample_uuid' for the analysis that were missing.
    """
    return arrayItemsWorkflow(workflow_name, workflow_version_regex, regex, items,submitter_specimen_types, True)

def arrayContainingItemsWorkflow(workflow_name, workflow_version_regex, regex, items,submitter_specimen_types):
    """
    Returns a list of 'sample_uuid' for the analysis that were present.
    """
    return arrayItemsWorkflow(workflow_name, workflow_version_regex, regex, items,submitter_specimen_types, False)

def arrayItemsWorkflow(workflow_name, workflow_version_regex, regex, items,submitter_specimen_types, missing):
    """
    Returns a list of 'sample_uuid' for the analysis that were missing.
    """
    analysis_type = False
    results = []
    for specimen in items['specimen']:
        if re.search(regex, specimen['submitter_specimen_type']):
            submitter_specimen_types.append(specimen['submitter_specimen_type'])
            for sample in specimen['samples']:
                for analysis in sample['analysis']:

                    if analysis["workflow_name"] == workflow_name and re.search(workflow_version_regex, analysis["workflow_version"]):
                        analysis_type = True
                        break

                if (missing and not analysis_type) or (not missing and analysis_type):
                    results.append(sample['sample_uuid'])

                analysis_type = False

    return results

def createFlags(uuid_to_donor):
    """
    uuid_to_donor: dictionary that maps uuid with its json object.
    Creates and adds "flags" and "missing_items" to each donor object.
    """
    for uuid in uuid_to_donor:
        json_object = uuid_to_donor[uuid]
        submitter_specimen_types=[]
        flagsWithArrs = {'normal_sequence': arrayMissingItems('sequence_upload', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_sequence': arrayMissingItems('sequence_upload',
                                                             "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line - ",
                                                             json_object,submitter_specimen_types),
                         'normal_sequence_qc_report': arrayMissingItems('sequence_upload_qc_report', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_sequence_qc_report': arrayMissingItems('sequence_upload_qc_report',
                                                             "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                             json_object,submitter_specimen_types),

                         'normal_alignment': arrayMissingItems('alignment', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_alignment': arrayMissingItems('alignment',
                                                              "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                              json_object,submitter_specimen_types),
                         'normal_alignment_qc_report': arrayMissingItems('alignment_qc_report', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_alignment_qc_report': arrayMissingItems('alignment_qc_report',
                                                              "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                              json_object,submitter_specimen_types),

                         'normal_rna_seq_quantification': arrayMissingItems('rna_seq_quantification', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_rna_seq_quantification': arrayMissingItems('rna_seq_quantification',
                                                                    "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                                    json_object,submitter_specimen_types),

                         'normal_rna_seq_cgl_workflow_3_0_x': arrayMissingItemsWorkflow('quay.io/ucsc_cgl/rnaseq-cgl-pipeline', '3\.0\.', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_rna_seq_cgl_workflow_3_0_x': arrayMissingItemsWorkflow('quay.io/ucsc_cgl/rnaseq-cgl-pipeline', '3\.0\.',
                                                                    "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                                    json_object,submitter_specimen_types),

                         'normal_germline_variants': arrayMissingItems('germline_variant_calling', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_somatic_variants': arrayMissingItems('somatic_variant_calling',
                                                                     "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                                     json_object,submitter_specimen_types)}

        flagsPresentWithArrs = {'normal_sequence': arrayContainingItems('sequence_upload', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_sequence': arrayContainingItems('sequence_upload',
                                                             "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                             json_object,submitter_specimen_types),
                         'normal_sequence_qc_report': arrayContainingItems('sequence_upload_qc_report', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_sequence_qc_report': arrayContainingItems('sequence_upload_qc_report',
                                                             "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                             json_object,submitter_specimen_types),

                         'normal_alignment': arrayContainingItems('alignment', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_alignment': arrayContainingItems('alignment',
                                                              "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                              json_object,submitter_specimen_types),
                         'normal_alignment_qc_report': arrayContainingItems('alignment_qc_report', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_alignment_qc_report': arrayContainingItems('alignment_qc_report',
                                                              "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                              json_object,submitter_specimen_types),

                         'normal_rna_seq_quantification': arrayContainingItems('rna_seq_quantification', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_rna_seq_quantification': arrayContainingItems('rna_seq_quantification',
                                                                    "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                                    json_object,submitter_specimen_types),

                         'normal_rna_seq_cgl_workflow_3_0_x': arrayContainingItemsWorkflow('quay.io/ucsc_cgl/rnaseq-cgl-pipeline', '3\.0\.', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_rna_seq_cgl_workflow_3_0_x': arrayContainingItemsWorkflow('quay.io/ucsc_cgl/rnaseq-cgl-pipeline', '3\.0\.',
                                                                    "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                                    json_object,submitter_specimen_types),

                         'normal_germline_variants': arrayContainingItems('germline_variant_calling', "^Normal - ", json_object,submitter_specimen_types),
                         'tumor_somatic_variants': arrayContainingItems('somatic_variant_calling',
                                                                     "^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Xenograft - |^Cell line -",
                                                                     json_object,submitter_specimen_types)}

        flagsWithStr = {'normal_sequence' : len(flagsWithArrs["normal_sequence"]) == 0 and len(flagsPresentWithArrs["normal_sequence"]) > 0,
                        'normal_sequence_qc_report' : len(flagsWithArrs["normal_sequence_qc_report"]) == 0 and len(flagsPresentWithArrs["normal_sequence_qc_report"]) > 0,
                        'tumor_sequence': len(flagsWithArrs["tumor_sequence"]) == 0 and len(flagsPresentWithArrs["tumor_sequence"]) > 0,
                        'tumor_sequence_qc_report' :len(flagsWithArrs["tumor_sequence_qc_report"]) == 0 and len(flagsPresentWithArrs["tumor_sequence_qc_report"]) > 0,
                        'normal_alignment': len(flagsWithArrs["normal_alignment"]) == 0 and len(flagsPresentWithArrs["normal_alignment"]) > 0,
                        'normal_alignment_qc_report': len(flagsWithArrs["normal_alignment_qc_report"]) == 0 and len(flagsPresentWithArrs["normal_alignment_qc_report"]) > 0,
                        'tumor_alignment': len(flagsWithArrs["tumor_alignment"]) == 0 and len(flagsPresentWithArrs["tumor_alignment"]) > 0,
                        'tumor_alignment_qc_report': len(flagsWithArrs["tumor_alignment_qc_report"]) == 0 and len(flagsPresentWithArrs["tumor_alignment_qc_report"]) > 0,
                        'normal_rna_seq_quantification': len(flagsWithArrs["normal_rna_seq_quantification"]) == 0 and len(flagsPresentWithArrs["normal_rna_seq_quantification"]) > 0,
                        'tumor_rna_seq_quantification': len(flagsWithArrs["tumor_rna_seq_quantification"]) == 0 and len(flagsPresentWithArrs["tumor_rna_seq_quantification"]) > 0,
                        'normal_rna_seq_cgl_workflow_3_0_x': len(flagsWithArrs["normal_rna_seq_cgl_workflow_3_0_x"]) == 0 and len(flagsPresentWithArrs["normal_rna_seq_cgl_workflow_3_0_x"]) > 0,
                        'tumor_rna_seq_cgl_workflow_3_0_x': len(flagsWithArrs["tumor_rna_seq_cgl_workflow_3_0_x"]) == 0 and len(flagsPresentWithArrs["tumor_rna_seq_cgl_workflow_3_0_x"]) > 0,

                        'normal_germline_variants': len(flagsWithArrs["normal_germline_variants"]) == 0 and len(flagsPresentWithArrs["normal_germline_variants"]) > 0,
                        'tumor_somatic_variants': len(flagsWithArrs["tumor_somatic_variants"]) == 0 and len(flagsPresentWithArrs["tumor_somatic_variants"]) > 0}

        json_object['flags'] = flagsWithStr
        json_object['missing_items'] = flagsWithArrs
        json_object['present_items'] = flagsPresentWithArrs


def dumpResult(result, filename, ES_file_name="elasticsearch.jsonl"):
    """
    Creates the .jsonl files.
    """
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

def findRedactedUuids(skip_uuid_directory):
    """
    Creates a dict of file UUIDs that need to be skipped
    """
    result = {}
    if skip_uuid_directory is not None:
        for file in os.listdir(skip_uuid_directory):
            if file.endswith(".redacted"):
                current_file = os.path.join(skip_uuid_directory, file)
                f = open(current_file, "r")
                for line in f.readlines():
                    result[line.rstrip()] = True
                f.close()
    print result
    return result

def main():
    args = input_Options()
    directory_meta = args.test_directory
    #get_size_list(args.storage_access_token, args.server_host)
    # redacted metadata.json file UUIDs
    skip_uuid_directory = args.skip_uuid_directory
    skip_uuids = findRedactedUuids(skip_uuid_directory)
    preserve_version = args.preserve_version

    logfileName = os.path.basename(__file__).replace(".py", ".log")
    logging_format= '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=logfileName, level=logging.DEBUG, format=logging_format, datefmt='%m/%d/%Y %I:%M:%S %p')

    if not directory_meta:
        #Getting the File UUIDs
        requires(args.server_host)
        #Get the size listing
        file_uuid_and_size = get_size_list(args.storage_access_token, args.server_host)
        print "Checking if variable was assigned"
        print file_uuid_and_size[0] #TEST
        #Trying to download the data.
        last= False
        page=0
        obj_arr=[]

        # figure out the pages
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        json_str = urlopen(str("https://"+args.server_host+":8444/entities?fileName=metadata.json&page=0"), context=ctx).read()
        metadata_struct = json.loads(json_str)

        # Download all of the data that is stored.
        for page in range(0, metadata_struct["totalPages"]):
            print "DOWNLOADING PAGE "+str(page)
            meta_cmd= ["curl", "-k"]
            url= 'https://'+args.server_host+':8444/entities?fileName=metadata.json&page='
            new_url=  url + str(page)
            meta_cmd.append(new_url)

            c_data=Popen(meta_cmd, stdout=PIPE, stderr=PIPE)
            stdout, stderr = c_data.communicate()
            json_obj= json.loads(stdout)
            last = json_obj["last"]
            obj_arr.append(json_obj)


        # Create a mapping of all the data provided from the endpoint.
        id_to_content= endpint_mapping(obj_arr)

        # Download the metadata.json files using the id stored in id_to_content dictionary
        directory_meta= make_output_dir()
        access_Token=args.storage_access_token
        client_Path= args.client_path
        create_merge_input_folder(id_to_content, directory_meta,access_Token,client_Path, file_uuid_and_size)

        # END DOWNLOAD

    # BEGIN json Merge
    logging.info("Begin Merging.")
    print "Begin Merging."
    schema = load_json_obj(args.metadata_schema)

    #if there is no schema the program cannot continue.
    if schema == None:
        logging.critical("No metadata schema was recognized. Exiting program.")
        exit(1)

    schema_version= schema["definitions"]["schema_version"]["pattern"]
    #sche_version= schema_version.replace("^","")
    #schema_version= sche_version.replace("$","")
    logging.info("Schema Version: %s" % schema_version)
    print "Schema Version: ",schema_version
    data_arr = []

    # Loads the json files and stores them into an array.
    load_json_arr(directory_meta, data_arr, skip_uuids)


    donorLevelObjs = []
    detachedObjs = []
    # Separates the detached anlaysis obj from the donor obj.
    for metaobj in data_arr:
        if "donor_uuid" in metaobj:
            donorLevelObjs.append(metaobj)
        elif "parent_uuids" in metaobj:
            detachedObjs.append(metaobj)

    # Skip Program Test Option.
    skip_prog_option= args.skip_program
    if skip_prog_option:
        logging.info("Skip Programs with values: %s" % (skip_prog_option))
        print "Skip Programs with values: %s" % (skip_prog_option)
        skip_option(donorLevelObjs, skip_prog_option,'program')

    # Use Only Program Test Option.
    only_program_option= args.only_program
    if only_program_option:
        logging.info("Only use Programs with values: %s" % (only_program_option))
        print "Only use Programs with values: %s" % (only_program_option)
        only_option(donorLevelObjs,only_program_option,'program')

    # Skip Program Test Option.
    skip_project_option= args.skip_project
    if skip_project_option:
        logging.info("Skip Projects with values: %s" % (skip_project_option))
        print "Skip Projects with values: %s" % (skip_project_option)
        skip_option(donorLevelObjs, skip_project_option,"project")

    # Use Only Program Test Option.
    only_project_option= args.only_project
    if only_project_option:
        logging.info("Only use Projects with values: %s" % (only_project_option))
        print "Only use Projects with values: %s" % (only_project_option)
        only_option(donorLevelObjs,only_project_option,"project")

    # Merge only those that are of the same schema_version as the Schema.
    invalid_version_arr= []
    valid_version_arr= []
    for donor_object in donorLevelObjs:
        obj_schema_version= donor_object["schema_version"]
        p = re.compile(schema_version)
        if not p.match(obj_schema_version):
            invalid_version_arr.append(donor_object)
        else:
            valid_version_arr.append(donor_object)
    logging.info("%s valid donor objects with correct schema version." % str(len(valid_version_arr)))
    print len(valid_version_arr), " valid donor objects with correct schema version."

    # Inserts the detached analysis to the merged donor obj.
    uuid_mapping = mergeDonors(valid_version_arr, preserve_version)
    for de_obj in detachedObjs:
        insert_detached_metadata(de_obj, uuid_mapping, preserve_version)

    # Creates and adds the flags and missingItems to each donor obj.
    createFlags(uuid_mapping)

    # Validates each donor obj
    (validated, invalid) = validate_Donor(uuid_mapping,schema)

    # Check if there are invalid json objects.
    invalid_num= len(invalid)
    if invalid_num:
        logging.info("%s merged donor objects invalid." % (invalid_num))
        print "%s merged donor objects invalid." % (invalid_num)
        dumpResult(invalid, "invalid.jsonl")
        logging.info("Invalid merged objects in invalid.jsonl.")
        print "Invalid merged objects in invalid.jsonl. "

    # Creates the jsonl files .
    validated_num= len(validated)
    if validated_num:
        logging.info("%s merged json objects were valid." % (validated_num))
        print  "%s merged json objects were valid." % (validated_num)
    if preserve_version:
        dumpResult(validated, "duped_validated.jsonl")
        dumpResult(validated, 'duped_elasticsearch.jsonl', ES_file_name="duped_elasticsearch.jsonl")
        logging.info("All done, find index in duped_elasticsearch.jsonl")
        print "All done, find index in duped_elasticsearch.jsonl"
    else:
        dumpResult(validated, "validated.jsonl")
        dumpResult(validated, 'elasticsearch.jsonl')
        logging.info("All done, find index in elasticsearch.jsonl")
        print "All done, find index in elasticsearch.jsonl"

    if not validated:
        logging.info("No objects were merged.")
        print "No objects were merged."


if __name__ == "__main__":
    main()
