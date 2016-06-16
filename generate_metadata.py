# generage_metadata.py
# Generate metadata files from a tsv file.
# See "helper client" in https://ucsc-cgl.atlassian.net/wiki/display/DEV/Storage+Service+-+Functional+Spec
# MAY2016	chrisw

# imports
import logging
from optparse import OptionParser
import sys
import csv
import os
import errno
import jsonschema
import openpyxl
import json
import uuid
from sets import Set
# import shutil
import subprocess
import time


# import re

# methods and functions

def getOptions():
    """
    parse options
    """
    usage_text = []
    usage_text.append("%prog [options] [input Excel or tsv files]")
    usage_text.append("Data will be read from 'Sheet1' in the case of Excel file.")

    parser = OptionParser(usage="\n".join(usage_text))
    parser.add_option("-v", "--verbose", action="store_true", default=False, dest="verbose",
                      help="Switch for verbose mode.")

    parser.add_option("-s", "--skip-upload", action="store_true", default=False, dest="skip_upload",
                      help="Switch to skip upload. Metadata files will be generated only.")

    parser.add_option("-m", "--metadataSchema", action="store", default="metadata_flattened.json", type="string",
                      dest="metadataSchemaFileName", help="flattened json schema file for metadata")

    parser.add_option("-d", "--outputDir", action="store", default="output_metadata", type="string",
                      dest="metadataOutDir",
                      help="output directory. In the case of colliding file names, the older file will be overwritten.")

    (options, args) = parser.parse_args()

    return (options, args, parser)

def getTimestamp():
    stamp = time.time()
    return stamp

def loadJsonObj(fileName):
    """
    Load a json object from a file.
    """
    try:
        file = open(fileName, "r")
        object = json.load(file)
        file.close()
    except Exception as exc:
        logging.exception("loadJsonObj")
    return object

def loadJsonSchema(fileName):
    """
    Load a json schema (actually just an object) from a file.
    """
    schema = loadJsonObj(fileName)
    return schema

def validateObjAgainstJsonSchema(obj, schema):
    """
    Validate an object against a schema.
    """
    try:
        jsonschema.validate(obj, schema)
    except Exception as exc:
        logging.error("jsonschema.validate FAILED in validateObjAgainstJsonSchema: %s" % (str(exc)))
        return False
    return True


def readFileLines(filename, strip=True):
    """
    Convenience method for getting an array of fileLines from a file.
    """
    fileLines = []
    file = open(filename, 'r')
    for line in file.readlines():
        if strip:
            line = line.rstrip("\r\n")
        fileLines.append(line)
    file.close()
    return fileLines


def readTsv(fileLines, d="\t"):
    """
    convenience method for reading TSV file lines into csv.DictReader obj.
    """
    reader = csv.DictReader(fileLines, delimiter=d)
    return reader


def normalizePropertyName(inputStr):
    """
    field names in the schema are all lower-snake-case
    """
    newStr = inputStr.lower()
    newStr = newStr.replace(" ", "_")
    return newStr


def processFieldNames(dictReaderObj):
    """
    normalize the field names in a DictReader obj
    """
    newDataList = []
    for dict in dictReaderObj:
        newDict = {}
        newDataList.append(newDict)
        for key in dict.keys():
            newKey = normalizePropertyName(key)
            newDict[newKey] = dict[key]
    return newDataList


def setUuids(dataObj):
    """
    Set donor_uuid, specimen_uuid, and sample_uuid for dataObj. Uses uuid.uuid5().
    """
    keyFieldsMapping = {}
    keyFieldsMapping["donor_uuid"] = ["center_name", "submitter_donor_id"]

    keyFieldsMapping["specimen_uuid"] = list(keyFieldsMapping["donor_uuid"])
    keyFieldsMapping["specimen_uuid"].append("submitter_specimen_id")

    keyFieldsMapping["sample_uuid"] = list(keyFieldsMapping["specimen_uuid"])
    keyFieldsMapping["sample_uuid"].append("submitter_sample_id")

    for uuidName in keyFieldsMapping.keys():
        keyList = []
        for field in keyFieldsMapping[uuidName]:
            if dataObj[field] is None:
                return None
            keyList.append(dataObj[field])
        # was having some trouble with data coming out of openpyxl not being ascii
        s = "".join(keyList).encode('ascii', 'ignore').lower()
        dataObj[uuidName] = str(uuid.uuid5(uuid.NAMESPACE_URL, s))

def getWorkflowUuid(sample_uuid, workflow_name, workflow_version):
    """
    Get a workflowUuid for use in this script.
    """
    keyList = []
    keyList.append(sample_uuid)
    keyList.append(workflow_name)
    keyList.append(workflow_version)
    s = "".join(keyList).encode('ascii', 'ignore').lower()
    workflowUuid = str(uuid.uuid5(uuid.NAMESPACE_URL, s))
    return workflowUuid

def getDataObj(dict, schema):
    """
    Pull data out from dict. Use the flattened schema to get the key names as well as validate.
    If validation fails, return None.
    """
    setUuids(dict)

    propNames = schema["properties"].keys()

    dataObj = {}
    for propName in propNames:
        dataObj[propName] = dict[propName]

    isValid = validateObjAgainstJsonSchema(dataObj, schema)
    if (isValid):
        return dataObj
    else:
        logging.error("validation FAILED for \t%s\n" % (json.dumps(dataObj, sort_keys=True, indent=4, separators=(',', ': '))))
        return None


def getDataDictFromXls(fileName, sheetName="Sheet1"):
    """
    Get list of dict objects from .xlsx,.xlsm,.xltx,.xltm.
    """
    logging.debug("attempt to read %s as xls file\n" % (fileName))
    workbook = openpyxl.load_workbook(fileName)
    sheetNames = workbook.get_sheet_names()
    logging.debug("sheetNames:\t%s\n" % (str(sheetNames)))

    worksheet = workbook.get_sheet_by_name(sheetName)

    headerRow = worksheet.rows[0]
    dataRows = worksheet.rows[1:]

    # map column index to column name
    colMapping = {}
    for colIdx in xrange(len(headerRow)):
        cell = headerRow[colIdx]
        value = cell.value
        if (value != None):
            colMapping[colIdx] = normalizePropertyName(value)

    # build up list of row data objs
    data = []
    for row in dataRows:
        rowDict = {}
        data.append(rowDict)
        for colIdx in colMapping.keys():
            colName = colMapping[colIdx]
            value = row[colIdx].value
            rowDict[colName] = value

    return data

def ln_s(file_path, link_path):
    """
    ln -s
    note: will not clobber existing file
    """
    try:
        os.symlink(file_path, link_path)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            if os.path.isdir(link_path):
                logging.error("ln_s\t linking failed -> %s is an existing directory" % (link_path))
            elif os.path.isfile(link_path):
                logging.error("ln_s\t linking failed -> %s is an existing file" % (link_path))
            elif os.path.islink(link_path):
                logging.error("ln_s\t linking failed -> %s is an existing link" % (link_path))
        else:
            logging.error("raising error")
            raise
    return None

def mkdir_p(path):
    """
    mkdir -p
    """
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
    return None

def getObj(dataObjs, queryObj):
    """
    Check to see if queryObj is contained in some object in dataObjs by checking for identical key:value pairs.
    If match is found, return the matched object in dataObjs.
    If no match is found, return None.
    """
    for dataObj in dataObjs:
        foundMismatch = False
        if not isinstance(dataObj, dict):
            continue
        for key in queryObj.keys():
            queryVal = queryObj[key]
            dataVal = dataObj[key]
            if queryVal != dataVal:
                foundMismatch = True
                break
        if foundMismatch == True:
            continue
        else:
            return dataObj

    return None

def getDonorLevelObjects(metadataObjs):
    """
    For each flattened metadata object, build up a metadataObj with correct structure.
    """
    num_files_written = 0

    commonObjMap = {}
    for metaObj in metadataObjs:
        commonObj = {}
        commonObj["program"] = metaObj["program"]
        commonObj["project"] = metaObj["project"]
        commonObj["center_name"] = metaObj["center_name"]
        commonObj["submitter_donor_id"] = metaObj["submitter_donor_id"]
        commonObj["donor_uuid"] = metaObj["donor_uuid"]
        commonObj["specimen"] = []

        # get donor level obj
        commonObjS = json.dumps(commonObj, sort_keys=True)
        if not commonObjS in commonObjMap.keys():
            commonObjMap[commonObjS] = commonObj
            commonObjMap[commonObjS]["timestamp"] = getTimestamp()
            commonObjMap[commonObjS]["schema_version"] = "0.0.1"

        # add specimen
        specObj = {}
        specObj["submitter_specimen_id"] = metaObj["submitter_specimen_id"]
        specObj["submitter_specimen_type"] = metaObj["submitter_specimen_type"]
        specObj["specimen_uuid"] = metaObj["specimen_uuid"]

        storedSpecObj = getObj(commonObjMap[commonObjS]["specimen"], specObj)

        if storedSpecObj == None:
            commonObjMap[commonObjS]["specimen"].append(specObj)
            storedSpecObj = getObj(commonObjMap[commonObjS]["specimen"], specObj)
            storedSpecObj["samples"] = []

        # add sample
        sampleObj = {}
        sampleObj["submitter_sample_id"] = metaObj["submitter_sample_id"]
        sampleObj["sample_uuid"] = metaObj["sample_uuid"]

        storedSampleObj = getObj(storedSpecObj["samples"], sampleObj)
        if storedSampleObj == None:
            storedSpecObj["samples"].append(sampleObj)
            storedSampleObj = getObj(storedSpecObj["samples"], sampleObj)

        # add workflow
        workFlowObj = {}
        workFlowObj["workflow_name"] = metaObj["workflow_name"]
        workFlowObj["workflow_version"] = metaObj["workflow_version"]
        workFlowObj["analysis_type"] = metaObj["analysis_type"]

        storedWorkFlowObj = getObj(storedSampleObj.values(), workFlowObj)
        if storedWorkFlowObj == None:
            analysis_type = metaObj["analysis_type"]
            storedSampleObj[analysis_type] = workFlowObj
            storedWorkFlowObj = getObj(storedSampleObj.values(), workFlowObj)
            storedWorkFlowObj["workflow_outputs"] = []

        # add file info
        fileInfoObj = {}
        fileInfoObj["file_type"] = metaObj["file_type"]
        fileInfoObj["file_path"] = metaObj["file_path"]

        storedFileInfoObj = getObj(storedWorkFlowObj["workflow_outputs"] , fileInfoObj)
        if storedFileInfoObj == None:
            storedWorkFlowObj["workflow_outputs"].append(fileInfoObj)
        else:
            logging.warning("skipping duplicate workflow_output for %s" % (json.dumps(metaObj, indent=4, separators=(',', ': '), sort_keys=True)))

    return commonObjMap.values()

def writeJson(directory, fileName, jsonObj):
    """
    Dump a json object to the specified directory/fileName. Creates directory if necessary.
    NOTE: will clobber the existing file
    """
    success = None
    try:
        mkdir_p(directory)
        filePath = os.path.join(directory, fileName)
        file = open(filePath, 'w')
        json.dump(jsonObj, file, indent=4, separators=(',', ': '), sort_keys=True)
        success = 1
    except Exception as exc:
        logging.exception("ERROR writing %s/%s\n" % (directory, fileName))
        success = 0
    finally:
        file.close()
    return success

def writeMetadataOutput(structuredDonorLevelObjs, outputDir):
    """
    For each structuredDonorLevelObj...
      1. extract and write the biospecimen.json
      2. extract and write the metadata.json files for each workflow
      3. link the data files
    """
    numFilesWritten = 0
    for donorLevelObj in structuredDonorLevelObjs:
        timestamp = donorLevelObj["timestamp"]
        schema_version = donorLevelObj["schema_version"]
        donorPath = os.path.join(outputDir, donorLevelObj["donor_uuid"])
#         numFilesWritten += writeJson(donorPath, "donor.json", donorLevelObj)
        specimens = donorLevelObj["specimen"]
        for specimen in donorLevelObj["specimen"]:
           for sample in specimen["samples"]:
               for sampleKey in sample.keys():
                   sample_uuid = sample["sample_uuid"]
                   obj = sample[sampleKey]
                   if (isinstance(obj, dict)) and ("analysis_type" in obj.keys()):
                       analysis_type = obj["analysis_type"]
                       workflowObj = sample.pop(sampleKey)

                       # update parent_uuids list...
                       if not "parent_uuids" in sample.keys():
                           workflowObj["parent_uuids"] = []
                       parent_uuids_set = set(workflowObj["parent_uuids"])
                       parent_uuids_set.add(sample_uuid)
                       workflowObj["parent_uuids"] = list(parent_uuids_set)

                       # add timestamp and schema_version from the donorLevelObj
                       workflowObj["timestamp"] = timestamp
                       workflowObj["schema_version"] = schema_version

                       # link data file(s)
                       for file_info in workflowObj["workflow_outputs"]:
                           file_path = file_info["file_path"]
                           fullFilePath = os.path.join(os.getcwd(), file_path)
                           filename = os.path.basename(file_path)
                           linkPath = os.path.join(donorPath, filename)
                           mkdir_p(donorPath)
                           ln_s(fullFilePath, linkPath)

                       # write json file
                       workflowUuid = getWorkflowUuid(sample_uuid, workflowObj["workflow_name"], workflowObj["workflow_version"])
                       numFilesWritten += writeJson(donorPath, workflowUuid + "_" + analysis_type + ".json", workflowObj)
                   else:
                       continue

        numFilesWritten += writeJson(donorPath, "biospecimen.json", donorLevelObj)

    return numFilesWritten

# parse output to retrieve "object id"
def parseUploadOutputForObjectIds(output):
    """
    1. Parse the output from ucsc-upload.sh to get the object ids of the upload.
    2. Return the discovered object ids.
    """
    ids = []
    for outputLine in output.split("\n"):
        fields = outputLine.split("using the object id")
        if len(fields) == 2:
            objectId = fields[1].strip()
            objectName = fields[0].strip()
            objectName = objectName.replace("Uploading object:", "", 1).strip()
            objectName = objectName.replace("'", "")
            objectInfo = {}
            objectInfo["id"] = objectId
            objectInfo["name"] = objectName
            ids.append(objectInfo)
    return ids

def uploadMultipleFilesViaExternalScript(filePaths):
    """
    1. Upload a multiple files with the ucsc-storage-client/ucsc-upload.sh script.
    2. Parse the output from ucsc-upload.sh to get the object ids of the uploads.
    3. Return a mapping of filePath to object id.
    """
    startTime = getTimestamp()

    # check correct file paths
    fullFilePaths = []
    if not os.path.isfile("ucsc-storage-client/ucsc-upload.sh"):
        logging.critical("missing file: %s\n" % ("ucsc-storage-client/ucsc-upload.sh"))
    for filePath in filePaths:
        fullFilePath = os.path.join(os.getcwd(), filePath)
        if not os.path.isfile(fullFilePath):
            logging.error("missing file: %s\n" % (fullFilePath))
        else:
            fullFilePaths.append(fullFilePath)

    # build command string
    command = ["/bin/bash", "ucsc-upload.sh"]
    command.extend(fullFilePaths)
    command = " ".join(command)
    logging.debug("command:\t%s\n" % (command))

    # execute script, capture output
    try:
        output = subprocess.check_output(command, cwd="ucsc-storage-client", stderr=subprocess.STDOUT, shell=True)
        logging.debug("output:%s\n" % (str(output)))
    except Exception as exc:
        logging.exception("ERROR while uploading multiple files")
        output = ""
    finally:
        logging.info("done uploading multiple files")

    # parse output for object ids
    objectIdInfo = parseUploadOutputForObjectIds(output)
    if len(objectIdInfo) != len(filePaths):
        logging.warning("number of object IDs does not match number of upload files: %s != %s" % (str(len(objectIdInfo)), str(len(filePaths))))

    runTime = startTime - getTimestamp()
    logging.info("upload took %s s." % (str(runTime)))

    return objectIdInfo

def uploadBiospecimenMetadata(uploadFilePath):
    """
    upload biospecimen metadata file. Returns 0 if failed, 1 if successful.s
    """
    ids = uploadMultipleFilesViaExternalScript([uploadFilePath])
    upload_uuid = ids[0]["id"]
    logging.info("upload_uuid for %s is %s" % (uploadFilePath, upload_uuid))
    if not upload_uuid == None:
        return 1
    else:
        logging.critical("Did not get an upload_uuid for %s" % (uploadFilePath))
        return 0

def uploadAnalysesMetadata(uploadFilePath):
    """
    upload analysis metadata file. Returns 0 if failed, 1 if successful.s
    """
    ids = uploadMultipleFilesViaExternalScript([uploadFilePath])
    upload_uuid = ids[0]["id"]
    logging.info("upload_uuid for %s is %s" % (uploadFilePath, upload_uuid))
    if not upload_uuid == None:
        return 1
    else:
        logging.critical("Did not get an upload_uuid for %s" % (uploadFilePath))
        return 0

def uploadWorkflowOutputFiles(metadataObj, dirName):
    """
    1. Find workflow_outputs.
    2. Upload each output file
    3. Update metadataObj with file_uuid of upload
    """
    num_uploads = 0
    for workflow_output in metadataObj["workflow_outputs"]:
        ids = uploadMultipleFilesViaExternalScript([workflow_output["file_path"]])
        file_uuid = ids[0]["id"]
        if (file_uuid != None):
            # upload failed
            num_uploads += 1
        workflow_output["file_uuid"] = file_uuid
    return num_uploads

#:####################################

def main():
    startTimestamp = getTimestamp()
    (options, args, parser) = getOptions()

    if len(args) == 0:
        logging.critical("no input files\n")
        sys.exit(1)

    verbose = options.verbose
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    logging.debug('options:\t%s\n' % (str(options)))
    logging.debug('args:\t%s\n' % (str(args)))

    # load flattened metadata schema
    metadataSchema = loadJsonSchema(options.metadataSchemaFileName)

    flatMetadataObjs = []

    # iter over input files
    for fileName in args:
        try:
            # attempt to process as xls file
            fileDataList = getDataDictFromXls(fileName)
        except Exception as exc:
            # attempt to process as tsv file
            logging.info("couldn't read %s as excel file\n" % fileName)
            logging.info("---now trying to read as tsv file\n")
            fileLines = readFileLines(fileName)
            reader = readTsv(fileLines)
            fileDataList = processFieldNames(reader)

        for data in fileDataList:
            metaObj = getDataObj(data, metadataSchema)

            if metaObj == None:
                continue

            flatMetadataObjs.append(metaObj)

    # get structured donor-level objects
    donorLevelObjs = getDonorLevelObjects(flatMetadataObjs)

    # write biospecimen.json and metadata.json files
    numFilesWritten = writeMetadataOutput(donorLevelObjs, options.metadataOutDir)
    logging.info("number of files written: %s\n" % (str(numFilesWritten)))

    if (options.skip_upload):
        sys.stderr.write("Skipping data upload steps.\n")
        return None
    else:
        sys.stderr.write("Now attempting to upload data.\n")

    # TODO: so this upload mechanism is not great, need to cleanup, need to use symlinks since cp will take a long time on big files
    # TODO section below is very broken
    uploadCounts = {}
    uploadCounts["workflowOutputs"] = 0
    uploadCounts["biospecimens"] = 0
    uploadCounts["analyses"] = 0
    numFilesWritten = 0
    for dirName, subdirList, fileList in os.walk(options.metadataOutDir):
        if dirName == options.metadataOutDir:
            continue
        sys.stderr.write('looking in directory: %s\n' % dirName)
        for fileName in fileList:
            if fileName.endswith(".json"):
                sys.stderr.write('\tfound %s\n' % fileName)
                filePath = os.path.join(dirName, fileName)
                if fileName == "biospecimen.json":
                    uploadCounts["biospecimens"] += uploadBiospecimenMetadata(filePath)
                else:
                    metadataObj = loadJsonObj(filePath)
                    uploadCounts["workflowOutputs"] += uploadWorkflowOutputFiles(metadataObj, dirName)
                    # write updated metadataObj ... contains upload_uuids
                    numFilesWritten += writeJson(dirName, fileName, metadataObj)
                    uploadCounts["analyses"] += uploadAnalysesMetadata(filePath)

    sys.stderr.write("uploadCounts\t%s\n" % (json.dumps(uploadCounts)))
    sys.stderr.write("numFilesWritten\t%s\n" % (str(numFilesWritten)))

    runTime = getTimestamp() - startTimestamp
    logging.info("program ran for %s s." % str(runTime))
    return None


# main program section
if __name__ == "__main__":
    main()
