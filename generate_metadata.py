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


def getDataDictFromXls(fileName):
    """
    Get list of dict objects from .xlsx,.xlsm,.xltx,.xltm.
    Looks in "Sheet1".
    """
    logging.debug("attempt to read %s as xls file\n" % (fileName))
    workbook = openpyxl.load_workbook(fileName)
    sheetNames = workbook.get_sheet_names()
    logging.debug("sheetNames:\t%s\n" % (str(sheetNames)))

    # Hopefully, it will always be "Sheet1" !
    worksheet = workbook.get_sheet_by_name("Sheet1")

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


def mkdir_p(path):
    """
    mkdir -p
    """
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
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

def writeOutput(metadataObjs, outputDir):
    """
    For each flattened metadata object:
       1. Build up a metadataObj with correct structure.
       2. Write metadataObj to metadata.json file.
       3. Return number of files written.
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
            storedSampleObj["workflows"] = []

        # add workflow
        workFlowObj = {}
        workFlowObj["workflow_name"] = metaObj["workflow_name"]
        workFlowObj["workflow_version"] = metaObj["workflow_version"]
        workFlowObj["analysis_type"] = metaObj["analysis_type"]

        storedWorkFlowObj = getObj(storedSampleObj["workflows"], workFlowObj)
        if storedWorkFlowObj == None:
            storedSampleObj["workflows"].append(workFlowObj)
            storedWorkFlowObj = getObj(storedSampleObj["workflows"], workFlowObj)
            storedWorkFlowObj["workflow_outputs"] = []

        # add file info
        fileInfoObj = {}
        fileInfoObj["file_type"] = metaObj["file_type"]
        fileInfoObj["file_path"] = metaObj["file_path"]

        storedFileInfoObj = getObj(storedWorkFlowObj["workflow_outputs"], fileInfoObj)
        if storedFileInfoObj == None:
            storedWorkFlowObj["workflow_outputs"].append(fileInfoObj)

    # write files
    for metaObj in commonObjMap.values():
        donor_uuid = metaObj["donor_uuid"]
        donorDir = os.path.join(outputDir, donor_uuid)

        num_files_written += writeJson(donorDir, "metadata.json", metaObj)
    return num_files_written

def writeJson(directory, fileName, jsonObj):
    """
    Dump a json object to the specified directory/fileName. Creates directory if necessary.
    """
    success = None
    # write metadata.json:
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

# parse output to retrieve "object id"
def parseUploadOutputForObjectIds(output):
    """
    1. Parse the output from ucsc-upload.sh to get the object id of the upload.
    2. Return the discovered object ids.
    """
    ids = []
    for outputLine in output.split("\n"):
        fields = outputLine.split("using the object id")
        if len(fields) == 2:
            objectId = fields[1].strip()
            ids.append(objectId)
    return ids

def uploadSingleFileViaScript(uploadFilePath):
    """
    1. Upload a single file with the ucsc-storage-client/ucsc-upload.sh script.
    2. Parse the output from ucsc-upload.sh to get the object id of the upload.
    3. Return the object id.
    """
    upload_uuid = None

    fullFilePath = os.path.join(os.getcwd(), uploadFilePath)

    # check correct file paths
    if not os.path.isfile(fullFilePath):
        logging.error("missing file: %s\n" % (fullFilePath))
    if not os.path.isfile("ucsc-storage-client/ucsc-upload.sh"):
        logging.critical("missing file: %s\n" % ("ucsc-storage-client/ucsc-upload.sh"))

    command = ["/bin/bash", "ucsc-upload.sh", str(fullFilePath)]
    command = " ".join(command)
    logging.debug("command:\t%s\n" % (command))
    try:
        # subprocess.check_output captures output
#         output = subprocess.check_output(command, cwd="ucsc-storage-client", stderr=subprocess.STDOUT, shell=True)
        # subprocess.check_call captures return code only
        returnCode = subprocess.check_call(command, cwd="ucsc-storage-client", stderr=subprocess.STDOUT, shell=True)
        logging.debug("returnCode: %s\n" % (returnCode))
        # faking some return output
        output = "Uploading object: '/private/var/folders/0_/6dtmmwcx7x16sdjhxshdcjrh0000gn/T/tmp.19jNUYs0/upload/61569BEE-7AFE-42C6-8EA4-00714D29027C/normal.bam' using the object id 92bd77fb-18bf-5db6-8b9f-611cb3df0dd4"
        logging.debug("output:%s\n" % (str(output)))
    except Exception as exc:
        logging.exception("ERROR while uploading %s: %s\n" % (uploadFilePath, str(exc)))
        output = ""
    finally:
        logging.info("done uploading %s\n" % (uploadFilePath))

    ids = parseUploadOutputForObjectIds(output)
    logging.info("ids:%s\n" % (str(ids)))
    if (len(ids) == 0):
        logging.error("didn't get any object id for %s\n" % (fullFilePath))
    elif (len(ids) > 1):
        logging.error("got multiple object ids for %s: %s\n" % (fullFilePath, str(ids)))
    elif len(ids) == 1:
        upload_uuid = ids[0]

    return upload_uuid

def uploadWorkflowOutputFiles(metadataObj, dirName):
    """
    1. Find workflow_outputs.
    2. Upload each output file
    3. Update metadataObj with file_uuid of upload
    """
    num_uploads = 0
    for specimen in metadataObj["specimen"]:
        specimen_uuid = specimen["specimen_uuid"]
        for sample in specimen["samples"]:
            sample_uuid = sample["sample_uuid"]
            for workflow in sample["workflows"]:
                workflow_name = workflow["workflow_name"]
                workflow_version = workflow["workflow_version"]
                for workflow_output in workflow["workflow_outputs"]:
                    file_uuid = uploadSingleFileViaScript(workflow_output["file_path"])
                    if (file_uuid != None):
                        # upload failed
                        num_uploads += 1
                    workflow_output["file_uuid"] = file_uuid
    return num_uploads

#:####################################

def main():
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

    metadataObjs = []

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

            metadataObjs.append(metaObj)

    # write output
    num_files_written = writeOutput(metadataObjs, options.metadataOutDir)
    sys.stderr.write("%s metadata files written to %s\n" % (str(num_files_written), options.metadataOutDir))

    if (options.skip_upload):
        return None

    sys.stderr.write("Now attempting to upload data.\n")

    # TODO: so this upload mechanism is not great, need to cleanup, need to use symlinks since cp will take a long time on big files
    uploadCounts = {}
    uploadCounts["workflowOutputs"] = 0
    uploadCounts["metadataJson"] = 0
    for dirName, subdirList, fileList in os.walk(options.metadataOutDir):
        if dirName == options.metadataOutDir:
            continue
        sys.stderr.write('looking in directory: %s\n' % dirName)
        for fileName in fileList:
            if (fileName == "metadata.json"):
                sys.stderr.write('\tfound %s\n' % fileName)
                filePath = os.path.join(dirName, fileName)
                metadataObj = loadJsonObj(filePath)

                uploadCounts["workflowOutputs"] += uploadWorkflowOutputFiles(metadataObj, dirName)
                uploadCounts["metadataJson"] += writeJson(dirName, fileName, metadataObj)

    sys.stderr.write("uploadCounts\t%s\n" % (json.dumps(uploadCounts)))

    return None


# main program section
if __name__ == "__main__":
    main()
