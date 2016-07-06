# generage_metadata.py
# Generate and upload UCSC Core Genomics data bundles from information passed via excel or tsv file.
#
# See "helper client" in https://ucsc-cgl.atlassian.net/wiki/display/DEV/Storage+Service+-+Functional+Spec
#
# The upload portion of this script requires external java8 jars and other files from the private S3 bucket at <https://s3-us-west-2.amazonaws.com/beni-dcc-storage-dev/ucsc-storage-client.tar.gz>
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
import subprocess
import datetime
import copy

# methods and functions

def getOptions():
    """
    parse options
    """
    usage_text = []
    usage_text.append("%prog [options] [input Excel or tsv files]")
    usage_text.append("Data will be read from 'Sheet1' in the case of Excel file.")

    parser = OptionParser(usage="\n".join(usage_text))
    parser.add_option("-v", "--verbose", action="store_true", default=False, dest="verbose", help="Switch for verbose mode.")
    parser.add_option("-s", "--skip-upload", action="store_true", default=False, dest="skip_upload", help="Switch to skip upload. Metadata files will be generated only.")
    parser.add_option("-t", "--test", action="store_true", default=False, dest="test", help="Switch for development testing.")

    parser.add_option("-i", "--inputMetadataSchema", action="store", default="input_metadata.json", type="string", dest="inputMetadataSchemaFileName", help="flattened json schema file for input metadata")
    parser.add_option("-m", "--metadataSchema", action="store", default="metadata_schema.json", type="string", dest="metadataSchemaFileName", help="flattened json schema file for metadata")

    parser.add_option("-d", "--outputDir", action="store", default="output_metadata", type="string", dest="metadataOutDir", help="output directory. In the case of colliding file names, the older file will be overwritten.")

    parser.add_option("-r", "--receiptFile", action="store", default="receipt.tsv", type="string", dest="receiptFile", help="receipt file name. This tsv file is the receipt of the upload, with UUIDs filled in.")

    parser.add_option("--awsAccessToken", action="store", default="12345678-abcd-1234-abcdefghijkl", type="string", dest="awsAccessToken", help="access token for AWS looks something like 12345678-abcd-1234-abcdefghijkl.")
    parser.add_option("--metadataServerUrl", action="store", default="https://storage.ucsc-cgl.org:8444", type="string", dest="metadataServerUrl", help="URL for metadata server.")
    parser.add_option("--storageServerUrl", action="store", default="https://storage.ucsc-cgl.org:5431", type="string", dest="storageServerUrl", help="URL for storage server.")
    parser.add_option("--force-upload", action="store_true", default=False, dest="force_upload", help="Switch to force upload in case object ID already exists remotely. Overwrites existing bundle.")

    (options, args) = parser.parse_args()

    return (options, args, parser)

def jsonPP(obj):
    """
    Get a pretty stringified JSON
    """
    str = json.dumps(obj, indent=4, separators=(',', ': '), sort_keys=True)
    return str

def getNow():
    """
    Get a datetime object for utc NOW.
    Convert to ISO 8601 format with datetime.isoformat()
    """
    now = datetime.datetime.utcnow()
    return now

def getTimeDelta(startDatetime):
    """
    get a timedelta object. Get seconds elapsed with timedelta.total_seconds().
    """
    endDatetime = datetime.datetime.utcnow()
    timedeltaObj = endDatetime - startDatetime
    return timedeltaObj

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
    newStr = inputStr.encode('ascii', 'ignore').lower()
    newStr = newStr.replace(" ", "_")
    newStr = newStr.strip()
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

def generateUuid5(nameComponents, namespace=uuid.NAMESPACE_URL):
    """
    generate a uuid5 where the name is the lower case of concatenation of nameComponents
    """
    strings = []
    for nameComponent in nameComponents:
        # was having some trouble with data coming out of openpyxl not being ascii
        strings.append(nameComponent.encode('ascii', 'ignore'))
    name = "".join(strings).lower()
    id = str(uuid.uuid5(namespace, name))
    return id

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

#     keyFieldsMapping["workflow_uuid"] = ["sample_uuid", "workflow_name", "workflow_version"]

    for uuidName in keyFieldsMapping.keys():
        keyList = []
        for field in keyFieldsMapping[uuidName]:
            if dataObj[field] is None:
                logging.error("%s not found in %s" % (field, jsonPP(dataObj)))
                return None
            else:
                keyList.append(dataObj[field])
        id = generateUuid5(keyList)
        dataObj[uuidName] = id

    # must follow sample_uuid assignment
    workflow_uuid_keys = ["sample_uuid", "workflow_name", "workflow_version"]
    keyList = []
    for field in workflow_uuid_keys:
        if dataObj[field] is None:
            logging.error("%s not found in %s" % (field, jsonPP(dataObj)))
            return None
        else:
            keyList.append(dataObj[field])
    id = generateUuid5(keyList)
    dataObj["workflow_uuid"] = id

def getDataObj(dict, schema):
    """
    Pull data out from dict. Use the flattened schema to get the key names as well as validate.
    If validation fails, return None.
    """
    setUuids(dict)

#     schema["properties"]["workflow_uuid"] = {"type": "string"}
    propNames = schema["properties"].keys()

    dataObj = {}
    for propName in propNames:
        dataObj[propName] = dict[propName]

    if "workflow_uuid" in dict.keys():
        dataObj["workflow_uuid"] = dict["workflow_uuid"]

    isValid = validateObjAgainstJsonSchema(dataObj, schema)
    if (isValid):
        return dataObj
    else:
        logging.error("validation FAILED for \t%s" % (jsonPP(dataObj)))
        return None

def getDataDictFromXls(fileName, sheetName="Sheet1"):
    """
    Get list of dict objects from .xlsx,.xlsm,.xltx,.xltm.
    """
    logging.debug("attempt to read %s as xls file" % (fileName))
    workbook = openpyxl.load_workbook(fileName)
    sheetNames = workbook.get_sheet_names()
    logging.debug("sheetNames:\t%s" % (str(sheetNames)))

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
                logging.error("linking failed -> %s is an existing directory" % (link_path))
            elif os.path.isfile(link_path):
                logging.error("linking failed -> %s is an existing file" % (link_path))
            elif os.path.islink(link_path):
                logging.error("linking failed -> %s is an existing link" % (link_path))
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

def getWorkflowObjects(flatMetadataObjs):
    """
    For each flattened metadata object, build up a metadataObj with correct structure.
    """
    schema_version = "0.0.2"
    num_files_written = 0

    commonObjMap = {}
    for metaObj in flatMetadataObjs:
        workflow_uuid = metaObj["workflow_uuid"]
        if workflow_uuid in commonObjMap.keys():
            pass
        else:
            workflowObj = {}
            commonObjMap[workflow_uuid] = workflowObj
            workflowObj["program"] = metaObj["program"]
            workflowObj["project"] = metaObj["project"]
            workflowObj["center_name"] = metaObj["center_name"]
            workflowObj["submitter_donor_id"] = metaObj["submitter_donor_id"]
            workflowObj["donor_uuid"] = metaObj["donor_uuid"]

            workflowObj["timestamp"] = getNow().isoformat()
            workflowObj["schema_version"] = schema_version

            workflowObj["specimen"] = []

            # add specimen
            specObj = {}
            workflowObj["specimen"].append(specObj)
            specObj["submitter_specimen_id"] = metaObj["submitter_specimen_id"]
            specObj["submitter_specimen_type"] = metaObj["submitter_specimen_type"]
            specObj["specimen_uuid"] = metaObj["specimen_uuid"]
            specObj["samples"] = []

            # add sample
            sampleObj = {}
            specObj["samples"].append(sampleObj)
            sampleObj["submitter_sample_id"] = metaObj["submitter_sample_id"]
            sampleObj["sample_uuid"] = metaObj["sample_uuid"]
            sampleObj["analysis"] = []

            # add workflow
            workFlowObj = {}
            sampleObj["analysis"].append(workFlowObj)
            workFlowObj["workflow_name"] = metaObj["workflow_name"]
            workFlowObj["workflow_version"] = metaObj["workflow_version"]
            workFlowObj["analysis_type"] = metaObj["analysis_type"]
            workFlowObj["workflow_outputs"] = []
            workFlowObj["bundle_uuid"] = metaObj["workflow_uuid"]

        # retrieve workflow
        workflowObj = commonObjMap[workflow_uuid]
        analysis_type = metaObj["analysis_type"]
        wf_outputsObj = workflowObj["specimen"][0]["samples"][0]["analysis"][0]["workflow_outputs"]

        # add file info
        fileInfoObj = {}
        wf_outputsObj.append(fileInfoObj)
        fileInfoObj["file_type"] = metaObj["file_type"]
        fileInfoObj["file_path"] = metaObj["file_path"]

    return commonObjMap

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
        logging.exception("ERROR writing %s/%s" % (directory, fileName))
        success = 0
    finally:
        file.close()
    return success

def writeDataBundleDirs(structuredMetaDataObjMap, outputDir):
    """
    For each structuredMetaDataObj, prepare a data bundle dir for the workflow.
    Assumes one data bundle per structuredMetaDataObj. That means 1 specimen, 1 sample, 1 analysis.
    """
    numFilesWritten = 0
    for workflow_uuid in structuredMetaDataObjMap.keys():
        metaObj = structuredMetaDataObjMap[workflow_uuid]

        # get outputDir (bundle_uuid)
        bundlePath = os.path.join(outputDir, workflow_uuid)

        # link data file(s)
        workflow_outputs = metaObj["specimen"][0]["samples"][0]["analysis"][0]["workflow_outputs"]
        for outputObj in workflow_outputs:
            file_path = outputObj["file_path"]
            fullFilePath = os.path.join(os.getcwd(), file_path)
            filename = os.path.basename(file_path)
            linkPath = os.path.join(bundlePath, filename)
            mkdir_p(bundlePath)
            ln_s(fullFilePath, linkPath)

        # write metadata
        numFilesWritten += writeJson(bundlePath, "metadata.json", metaObj)

    return numFilesWritten

def setupLogging(logfileName, logFormat, logLevel, logToConsole=True):
    """
    Setup simultaneous logging to file and console.
    """
#     logFormat = "%(asctime)s %(levelname)s %(funcName)s:%(lineno)d %(message)s"
    logging.basicConfig(filename=logfileName, level=logging.NOTSET, format=logFormat)
    if logToConsole:
        console = logging.StreamHandler()
        console.setLevel(logLevel)
        formatter = logging.Formatter(logFormat)
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
    return None

def registerBundleUpload(metadataUrl, bundleDir, accessToken):
     """
     java
         -Djavax.net.ssl.trustStore=ssl/cacerts
         -Djavax.net.ssl.trustStorePassword=changeit
         -Dserver.baseUrl=https://storage.ucsc-cgl.org:8444
         -DaccessToken=${accessToken}
         -jar dcc-metadata-client-0.0.16-SNAPSHOT/lib/dcc-metadata-client.jar
         -i ${upload}
         -o ${manifest}
         -m manifest.txt
     """
     success = True

     metadataClientJar = "ucsc-storage-client/dcc-metadata-client-0.0.16-SNAPSHOT/lib/dcc-metadata-client.jar"
     trustStore = "ucsc-storage-client/ssl/cacerts"
     trustStorePw = "changeit"

     # build command string
     command = ["java"]
     command.append("-Djavax.net.ssl.trustStore=" + trustStore)
     command.append("-Djavax.net.ssl.trustStorePassword=" + trustStorePw)
     command.append("-Dserver.baseUrl=" + str(metadataUrl))
     command.append("-DaccessToken=" + str(accessToken))
     command.append("-jar " + metadataClientJar)
     command.append("-i " + str(bundleDir))
     command.append("-o " + str(bundleDir))
     command.append("-m manifest.txt")
     command = " ".join(command)

     # !!! This may expose the access token !!!
#      logging.debug("register upload command:\t%s" % (command))

     try:
         output = subprocess.check_output(command, cwd=os.getcwd(), stderr=subprocess.STDOUT, shell=True)
         logging.debug("output:%s" % (str(output)))
     except Exception as exc:
         success = False
         # !!! logging.exception here may expose access token !!!
         logging.error("ERROR while registering bundle %s" % bundleDir)
         output = ""
     finally:
         logging.info("done registering bundle upload %s" % bundleDir)

     return success

def performBundleUpload(metadataUrl, storageUrl, bundleDir, accessToken, force=False):
    """
    Java
        -Djavax.net.ssl.trustStore=ssl/cacerts
        -Djavax.net.ssl.trustStorePassword=changeit
        -Dmetadata.url=https://storage.ucsc-cgl.org:8444
        -Dmetadata.ssl.enabled=true
        -Dclient.ssl.custom=false
        -Dstorage.url=https://storage.ucsc-cgl.org:5431
        -DaccessToken=${accessToken}
        -jar icgc-storage-client-1.0.14-SNAPSHOT/lib/icgc-storage-client.jar upload
        --manifest ${manifest}/manifest.txt
    """
    success = True

    storageClientJar = "ucsc-storage-client/icgc-storage-client-1.0.14-SNAPSHOT/lib/icgc-storage-client.jar"
    trustStore = "ucsc-storage-client/ssl/cacerts"
    trustStorePw = "changeit"

    # build command string
    command = ["java"]
    command.append("-Djavax.net.ssl.trustStore=" + trustStore)
    command.append("-Djavax.net.ssl.trustStorePassword=" + trustStorePw)
    command.append("-Dmetadata.url=" + str(metadataUrl))
    command.append("-Dmetadata.ssl.enabled=true")
    command.append("-Dclient.ssl.custom=false")
    command.append("-Dstorage.url=" + str(storageUrl))
    command.append("-DaccessToken=" + str(accessToken))
    command.append("-jar " + storageClientJar + " upload")

    # force upload in case object id already exists remotely
    if force:
        command.append("--force")

    manifestFilePath = os.path.join(bundleDir, "manifest.txt")
    command.append("--manifest " + manifestFilePath)
    command = " ".join(command)

    # !!! This may expose the access token !!!
#     logging.debug("perform upload command:\t%s" % (command))

    try:
        output = subprocess.check_output(command, cwd=os.getcwd(), stderr=subprocess.STDOUT, shell=True)
        logging.debug("output:%s" % (str(output)))
    except Exception as exc:
        success = False
        # !!! logging.exception here may expose access token !!!
        logging.error("ERROR while uploading files for bundle %s" % bundleDir)
        output = ""
    finally:
        logging.info("done uploading bundle %s" % bundleDir)

    return success

def parseUploadManifestFile(manifestFilePath):
    '''
    from the upload manifest file, get the file_uuid for each uploaded file
    '''
    bundle_uuid = None
    idMapping = {}

    fileLines = readFileLines(manifestFilePath)
    for line in fileLines:
        if bundle_uuid == None:
            # first line contains bundle_uuid
            fields = line.split(" ")
            bundle_uuid = fields[-1]
        elif line.startswith("#"):
            # skip comment lines
            pass
        else:
            # lines are in the form "file_uuid=file_path"
            fields = line.split("=", 1)
            fileName = os.path.basename(fields[1])
            idMapping[fileName] = fields[0]

    obj = {"bundle_uuid":bundle_uuid, "idMapping":idMapping}
    return obj

def collectReceiptData(manifestData, metadataObj):
    '''
    collect the data for the upload receipt file
    The required fields are:
    program project center_name submitter_donor_id donor_uuid submitter_specimen_id specimen_uuid submitter_specimen_type submitter_sample_id sample_uuid analysis_type workflow_name workflow_version file_type file_path file_uuid bundle_uuid metadata_uuid
    '''
    collectedData = []

    commonData = {}
    commonData["program"] = metadataObj["program"]
    commonData["project"] = metadataObj["project"]
    commonData["center_name"] = metadataObj["center_name"]
    commonData["submitter_donor_id"] = metadataObj["submitter_donor_id"]
    commonData["donor_uuid"] = metadataObj["donor_uuid"]

    commonData["submitter_specimen_id"] = metadataObj["specimen"][0]["submitter_specimen_id"]
    commonData["specimen_uuid"] = metadataObj["specimen"][0]["specimen_uuid"]
    commonData["submitter_specimen_type"] = metadataObj["specimen"][0]["submitter_specimen_type"]

    commonData["submitter_sample_id"] = metadataObj["specimen"][0]["samples"][0]["submitter_sample_id"]
    commonData["sample_uuid"] = metadataObj["specimen"][0]["samples"][0]["sample_uuid"]

    commonData["analysis_type"] = metadataObj["specimen"][0]["samples"][0]["analysis"][0]["analysis_type"]
    commonData["workflow_name"] = metadataObj["specimen"][0]["samples"][0]["analysis"][0]["workflow_name"]
    commonData["workflow_version"] = metadataObj["specimen"][0]["samples"][0]["analysis"][0]["workflow_version"]
    commonData["bundle_uuid"] = metadataObj["specimen"][0]["samples"][0]["analysis"][0]["bundle_uuid"]
    commonData["metadata_uuid"] = manifestData["idMapping"]["metadata.json"]

    workflow_outputs = metadataObj["specimen"][0]["samples"][0]["analysis"][0]["workflow_outputs"]
    for output in workflow_outputs:
        data = copy.deepcopy(commonData)
        data["file_type"] = output["file_type"]
        data["file_path"] = output["file_path"]

        fileName = os.path.basename(output["file_path"])
        data["file_uuid"] = manifestData["idMapping"][fileName]

        collectedData.append(data)

    return collectedData

def writeReceipt(collectedReceipts, receiptFileName, d="\t"):
    '''
    write an upload receipt file
    '''
    with open(receiptFileName, 'w') as receiptFile:
        fieldnames = ["program", "project", "center_name", "submitter_donor_id", "donor_uuid", "submitter_specimen_id", "specimen_uuid", "submitter_specimen_type", "submitter_sample_id", "sample_uuid", "analysis_type", "workflow_name", "workflow_version", "file_type", "file_path", "file_uuid", "bundle_uuid", "metadata_uuid"]
        writer = csv.DictWriter(receiptFile, fieldnames=fieldnames, delimiter=d)

        writer.writeheader()
        writer.writerows(collectedReceipts)
    return None

def validateMetadataObjs(metadataObjs, jsonSchemaFile):
    '''
    validate metadata objects
    '''
    schema = loadJsonSchema(jsonSchemaFile)
    valid = []
    invalid = []
    for metadataObj in metadataObjs:
        isValid = validateObjAgainstJsonSchema(metadataObj, schema)
        if isValid:
            valid.append(metadataObj)
        else:
            invalid.append(metadataObj)

    obj = {"valid":valid, "invalid":invalid}
    return obj

#:####################################

def main():
    startTime = getNow()
    (options, args, parser) = getOptions()

    if len(args) == 0:
        logging.critical("no input files")
        sys.exit(1)

    if options.verbose:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO
    logfileName = os.path.basename(__file__).replace(".py", ".log")
    logFormat = "%(asctime)s %(levelname)s %(funcName)s:%(lineno)d %(message)s"
    setupLogging(logfileName, logFormat, logLevel)

    # !!! careful not to expose the access token !!!
    printOptions = copy.deepcopy(vars(options))
    printOptions.pop("awsAccessToken")
    logging.debug('options:\t%s' % (str(printOptions)))
    logging.debug('args:\t%s' % (str(args)))

    tempDirName = os.path.basename(__file__) + "_temp"

    # load flattened metadata schema for input validation
    inputMetadataSchema = loadJsonSchema(options.inputMetadataSchemaFileName)

    flatMetadataObjs = []

    # iter over input files
    for fileName in args:
        try:
            # attempt to process as xls file
            fileDataList = getDataDictFromXls(fileName)
        except Exception as exc:
            # attempt to process as tsv file
            logging.info("couldn't read %s as excel file" % fileName)
            logging.info("---now trying to read as tsv file")
            fileLines = readFileLines(fileName)
            reader = readTsv(fileLines)
            fileDataList = processFieldNames(reader)

        for data in fileDataList:
            metaObj = getDataObj(data, inputMetadataSchema)

            if metaObj == None:
                continue

            flatMetadataObjs.append(metaObj)

    # get structured workflow objects
    structuredWorkflowObjMap = getWorkflowObjects(flatMetadataObjs)

    # validate metadata objects
    # exit script before upload
    validationResults = validateMetadataObjs(structuredWorkflowObjMap.values(), options.metadataSchemaFileName)
    numInvalidResults = len(validationResults["invalid"])
    if numInvalidResults != 0:
        logging.critical("%s invalid metadata objects found: %s" % (numInvalidResults, jsonPP(validationResults["invalid"])))
        sys.exit(1)
    else:
        logging.info("validated all metadata objects for output")

    # write metadata files and link data files
    numFilesWritten = writeDataBundleDirs(structuredWorkflowObjMap, options.metadataOutDir)
    logging.info("number of metadata files written: %s" % (str(numFilesWritten)))

    if (options.test):
        logging.info("code block for testing")
        bundle_uuids = ["1329abd5-a293-5a20-a32e-a31b7f37799d", "1b385686-29cd-5977-85af-7df005b7d561", "250036a8-51ca-5172-ab8a-b561061d7bc1", "27e5bf8e-b88c-5302-9c3a-57cc484808b2", "433c5ef9-e4db-5602-923c-1d454a9baad5", "6f50c297-72c9-582c-8434-5cee55bd76c5", "94879018-4c59-5537-a913-a87c820c69de", "b50a1bf3-588a-5549-a104-1602e6d45f70"]
        for bundle_uuid in bundle_uuids:
            filePath = os.path.join("output_metadata", bundle_uuid, "metadata.json")
            obj = loadJsonObj(filePath)
            donor_uuid = obj["donor_uuid"]
            logging.critical("donor_uuid: %s\tbundle_uuid: %s" % (jsonPP(donor_uuid), jsonPP(bundle_uuid)))
        return None

    if (options.skip_upload):
        logging.info("Skipping data upload steps.")
        runTime = getTimeDelta(startTime).total_seconds()
        logging.info("program ran for %s s." % str(runTime))
        return None
    else:
        logging.info("Now attempting to upload data.")
        logging.info("If the upload seems to hang, it could be that the server doesn't recognize the IP.")

    # UPLOAD SECTION
    counts = {}
    counts["bundlesFound"] = 0
    counts["failedRegistration"] = []
    counts["failedUploads"] = []
    counts["bundlesUploaded"] = 0

    # first pass uploads data bundles
    for dirName, subdirList, fileList in os.walk(options.metadataOutDir):
        if dirName == options.metadataOutDir:
            continue
        if len(subdirList) != 0:
            continue
        if "metadata.json" in fileList:
            bundleDirFullPath = os.path.join(os.getcwd(), dirName)
            logging.debug("found bundle directory at %s" % (bundleDirFullPath))
            counts["bundlesFound"] += 1

            bundle_uuid = dirName

            # register upload
            args = {"accessToken":options.awsAccessToken, "bundleDir":dirName, "metadataUrl":options.metadataServerUrl}
            regSuccess = registerBundleUpload(**args)

            # perform upload
            upSuccess = False
            if regSuccess:
                args["storageUrl"] = options.storageServerUrl
                args["force"] = options.force_upload
                upSuccess = performBundleUpload(**args)
            else:
                counts["failedRegistration"].append(bundle_uuid)

            if upSuccess:
                counts["bundlesUploaded"] += 1
            else:
                counts["failedUploads"].append(bundle_uuid)
        else:
            logging.info("no metadata file found in %s" % dirName)

    logging.info("counts\t%s" % (json.dumps(counts)))

    # second pass generates receipt.tsv
    logging.info("now generate upload receipt")
    collectedReceipts = []
    for dirName, subdirList, fileList in os.walk(options.metadataOutDir):
        if dirName == options.metadataOutDir:
            continue
        if len(subdirList) != 0:
            continue
        if "manifest.txt" in fileList:
            manifestFilePath = os.path.join(os.getcwd(), dirName, "manifest.txt")
            manifestData = parseUploadManifestFile(manifestFilePath)

            metadataFilePath = os.path.join(os.getcwd(), dirName, "metadata.json")
            metadataObj = loadJsonObj(metadataFilePath)

            receiptData = collectReceiptData(manifestData, metadataObj)
            for data in receiptData:
                collectedReceipts.append(data)
        else:
            logging.info("no manifest file found in %s" % dirName)
    writeReceipt(collectedReceipts, options.receiptFile)

    # final console output
    if len(counts["failedRegistration"]) > 0 or len(counts["failedUploads"]) > 0:
        logging.error("THERE WERE SOME FAILED PROCESSES !")

    runTime = getTimeDelta(startTime).total_seconds()
    logging.info("program ran for %s s." % str(runTime))
    logging.shutdown()
    return None

# main program section
if __name__ == "__main__":
    main()
