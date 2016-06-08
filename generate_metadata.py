# generage_metadata.py
# Generate metadata files from a tsv file.
# See "helper client" in https://ucsc-cgl.atlassian.net/wiki/display/DEV/Storage+Service+-+Functional+Spec
# MAY2016	chrisw

# imports
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
import shutil



# import re

# methods and functions

def prettyJson(obj):
    s = json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))
    return s

def getOptions():
    "parse options"
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


def log(msg, die=False):
    if (verbose | die):
        sys.stderr.write(msg)
    if die:
        sys.exit(1)


def loadJsonSchema(fileName):
    try:
        file = open(fileName, "r")
        schema = json.load(file)
        file.close()
    except Exception as exc:
        log("Exception:%s\n" % (str(exc)), die=True)
    return schema


def validateObjAgainstJsonSchema(obj, schema):
    try:
        jsonschema.validate(obj, schema)
    except Exception as exc:
        sys.stderr.write("Exception:%s\n" % (str(exc)))
        return False
    return True


def readFileLines(filename, strip=True):
    fileLines = []
    file = open(filename, 'r')
    for line in file.readlines():
        if strip:
            line = line.rstrip("\r\n")
        fileLines.append(line)
    file.close()
    return fileLines


def readTsv(fileLines, d="\t"):
    reader = csv.DictReader(fileLines, delimiter=d)
    return reader


def normalizePropertyName(inputStr):
    "field names in the schema are all lower-snake-case"
    newStr = inputStr.lower()
    newStr = newStr.replace(" ", "_")
    return newStr


def processFieldNames(dictReaderObj):
    "normalize the field names in a DictReader obj"
    newDataList = []
    for dict in dictReaderObj:
        newDict = {}
        newDataList.append(newDict)
        for key in dict.keys():
            newKey = normalizePropertyName(key)
            newDict[newKey] = dict[key]
    return newDataList


def setUuids(dataObj):
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
    "Pull data out from dict. Use the flattened schema to get the key names as well as validate."
    setUuids(dict)

    propNames = schema["properties"].keys()

    dataObj = {}
    for propName in propNames:
        dataObj[propName] = dict[propName]

    isValid = validateObjAgainstJsonSchema(dataObj, schema)
    if (isValid):
        return dataObj
    else:
        sys.stderr.write("validation FAILED for \t%s\n" % (json.dumps(dataObj, sort_keys=True, indent=4, separators=(',', ': '))))
        return None


def getDataDictFromXls(fileName):
    "can open .xlsx,.xlsm,.xltx,.xltm"
    log("attempt to read %s as xls file\n" % (fileName))
    workbook = openpyxl.load_workbook(fileName)
    sheetNames = workbook.get_sheet_names()
    log("sheetNames:\t%s\n" % (str(sheetNames)))

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
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
    return None

def getObj(dataObjs, queryObj):
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
        mkdir_p(donorDir)

        # write metadata.json
        filePath = os.path.join(donorDir, "metadata.json")
        file = open(filePath, 'w')
        json.dump(metaObj, file, indent=4, separators=(',', ': '))
        num_files_written += 1
        file.close()

    return num_files_written

#:####################################

def main():
    global verbose
    (options, args, parser) = getOptions()

    if len(args) == 0:
        sys.stderr.write("no input files\n")
        sys.exit(1)

    verbose = options.verbose
    log('options:\t%s\n' % (str(options)))
    log('args:\t%s\n' % (str(args)))

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
            sys.stderr.write("couldn't read %s as excel file\n" % fileName)
            sys.stderr.write("---now trying to read as tsv file\n")
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

    log("Now attempting to upload data.\n")

    # TODO: so this upload mechanism is not great, need to cleanup, need to use symlinks since cp will take a long time on big files
    num_files_uploaded = 0
    for dirName, subdirList, fileList in os.walk(options.metadataOutDir):
        if dirName == options.metadataOutDir:
            continue
        log('Found directory: %s\n' % dirName)
        for fileName in fileList:
            if (fileName.endswith("analysis.json")):
                log('\t%s\n' % fileName)
                filePath = os.path.join(dirName, fileName)

                # steps
                # 1) make an upload dir
                # 2) read this JSON
                # 3) copy each of the files in the JSON to upload dir
                # 4) modify JSON and write out to upload dir
                # 5) perform upload
                # 6) clean upload dir

                file = open(filePath, "r")
                metadataObj = json.load(file)
                file.close()
                if ("workflow_outputs" in metadataObj.keys()) and (metadataObj["workflow_outputs"].keys() > 0):
                    # make temp dir
                    upload_uuid = uuid.uuid4()
                    os.makedirs("uploads/" + str(upload_uuid))
                    for dataFileName in metadataObj["workflow_outputs"].keys():
                        log("dataFileName: %s\n" % (metadataObj["workflow_outputs"][dataFileName]["file_path"]))
                        file_basename = os.path.basename(metadataObj["workflow_outputs"][dataFileName]["file_path"])
                        file_full_path = metadataObj["workflow_outputs"][dataFileName]["file_path"]
                        # probably want to symlink in the future
                        shutil.copyfile(file_full_path, "uploads/" + str(upload_uuid) + "/" + file_basename)
                        metadataObj["workflow_outputs"][dataFileName]["file_path"] = file_basename
                        num_files_written += 1
                        # filePath = os.path.join(dataDir, metadataObj["workflow_outputs"])
                        num_files_uploaded += 1
                    afile = open("uploads/" + str(upload_uuid) + "/analysis.json", "w")
                    json.dump(metadataObj, afile, indent=4, separators=(',', ': '))
                    afile.close()
                    num_files_uploaded += 1
                    # TODO: assumes the upload client is here, not a safe assumption
                    # TODO: Emily, need to capture output here so I know the ID and Data Bundle ID
                    # TODO: directly use the tools in the future, the script below actually copies the inputs again!!
                    if os.path.isfile("ucsc-storage-client/ucsc-upload.sh"):
                        exit_code = os.system("cd ucsc-storage-client; bash ucsc-upload.sh ../uploads/" + str(upload_uuid) + "/*")
                        if exit_code != 0:
                            print "ERROR UPLOADING!!!!"
                            return None
                        else:
                            shutil.rmtree("uploads/" + str(upload_uuid))

    sys.stderr.write("%s files uploaded\n" % (str(num_files_uploaded)))

    return None


# main program section
if __name__ == "__main__":
    main()
