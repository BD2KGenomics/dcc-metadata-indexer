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
# import re

# methods and functions

def getOptions():
	"parse options"
	usage_text = []
	usage_text.append("%prog [options] [input Excel or tsv files]")
	usage_text.append("Data will be read from 'Sheet1' in the case of Excel file.")
	
	parser = OptionParser(usage="\n".join(usage_text))
	parser.add_option("-v", "--verbose", action="store_true", default=False, dest="verbose", help="Switch for verbose mode.")

	parser.add_option("-s", "--skip-upload", action="store_true", default=False, dest="skip_upload", help="Switch to skip upload. Metadata files will be generated only.")

	parser.add_option("-b", "--biospecimenSchema", action="store", default="biospecimen_flattened.json", type="string", dest="biospecimenSchemaFileName", help="flattened json schema file for biospecimen")
	parser.add_option("-a", "--analysisSchema", action="store", default="analysis_flattened.json", type="string", dest="analysisSchemaFileName", help="flattened json schema file for analysis")

	parser.add_option("-d", "--outputDir", action="store", default="output_metadata", type="string", dest="metadataOutDir", help="output directory. In the case of colliding file names, the older file will be overwritten.")


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
		sys.exit(1)

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

def writeOutput(donorSpecSampleBioMap, donorAnaMap, outputDir):
	num_files_written = 0
	num_files_written += writeBiospecimenOutput(donorSpecSampleBioMap, outputDir)
	num_files_written += writeAnalysisOutput(donorAnaMap, outputDir)
	return num_files_written

def writeBiospecimenOutput(donorSpecSampleBioMap, outputDir):
	num_files_written = 0
	for donor_uuid in donorSpecSampleBioMap.keys():
		donorDir = os.path.join(outputDir, donor_uuid)
		mkdir_p(donorDir)
		
		bioOutObj = {}
		
		specSampleBioMap = donorSpecSampleBioMap[donor_uuid]
		for specimen_uuid in specSampleBioMap.keys():
			sampleBioMap = specSampleBioMap[specimen_uuid]
			
			specimenObj = {}
			
			for sample_uuid in sampleBioMap.keys():
				bioObjStrings = sampleBioMap[sample_uuid]
				
				sampleObj = {}
				
				for bioObjString in bioObjStrings:
					bioObj = json.loads(bioObjString)
					specimen_uuid = bioObj["specimen_uuid"]
					
					if bioOutObj == {}:
						bioOutObj["program"] = bioObj["program"]
						bioOutObj["project"] = bioObj["project"]
						bioOutObj["center_name"] = bioObj["center_name"]
						bioOutObj["submitter_donor_id"] = bioObj["submitter_donor_id"]
						bioOutObj["donor_uuid"] = bioObj["donor_uuid"]
						bioOutObj["specimen"] = []
						
					if (specimenObj == {}):
						specimenObj["submitter_specimen_id"] = bioObj["submitter_specimen_id"]
						specimenObj["submitter_specimen_type"] = bioObj["submitter_specimen_type"]
						specimenObj["specimen_uuid"] = bioObj["specimen_uuid"]
						specimenObj["samples"] = []
						
						bioOutObj["specimen"].append(specimenObj)
						
					if (sampleObj == {}):
						sampleObj["submitter_sample_id"] = bioObj["submitter_sample_id"]
						sampleObj["sample_uuid"] = bioObj["sample_uuid"]
						specimenObj["samples"].append(sampleObj)
						
		# write biospecimen.json
		filePath = os.path.join(donorDir, "biospecimen.json")
		file = open(filePath, 'w')
		json.dump(bioOutObj, file, indent=4, separators=(',', ': '))
		num_files_written += 1
		file.close()

	return num_files_written

def writeAnalysisOutput(donorAnaMap, outputDir):
	num_files_written = 0
	for donor_uuid in donorAnaMap.keys():
		donorDir = os.path.join(outputDir, donor_uuid)
		mkdir_p(donorDir)
		
		anaObjs = donorAnaMap[donor_uuid]
		for idx in xrange(len(anaObjs)):
			anaObj = anaObjs[idx]
			anaOutObj = {}
			anaOutObj["parent_uuids"] = []
			anaOutObj["parent_uuids"].append(anaObj["sample_uuid"])
			anaOutObj["workflow_name"] = anaObj["workflow_name"]
			anaOutObj["workflow_version"] = anaObj["workflow_version"]
			
			anaOutObj["workflow_outputs"] = {}
			anaOutObj["workflow_outputs"][anaObj["file_path"]] = {}
			
			anaOutObj["workflow_outputs"][anaObj["file_path"]]["file_type_label"] = anaObj["file_type"]
			
			anaOutObj["analysis_type"] = anaObj["analysis_type"]
			
			# write biospecimen.json
			filePath = os.path.join(donorDir, str(idx) + "analysis.json")
			file = open(filePath, 'w')
			json.dump(anaOutObj, file, indent=4, separators=(',', ': '))
			num_files_written += 1
			file.close()
	
	return num_files_written

def uploadFile(fullFilePath):
	return 1

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

	# load bio schema
	bioSchema = loadJsonSchema(options.biospecimenSchemaFileName)

	# load analysis schema
	analysisSchema = loadJsonSchema(options.analysisSchemaFileName)

	# map donor_uuid to map of specimen_uuid to map of sample_uuid to list of bioObj
	donorSpecSampleBioMap = {}

	# map donor_uuid to list of anaObj
	donorAnaMap = {}

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
			# build and validate bio obj
			bioObj = getDataObj(data, bioSchema)

			# build and validate analysis obj
			anaObj = getDataObj(data, analysisSchema)

			# organize the data for assembling output objects
			bioDonor = bioObj["donor_uuid"]
			bioSpec = bioObj["specimen_uuid"]
			bioSample = bioObj["sample_uuid"]
			anaSample = anaObj["sample_uuid"]

			if (bioSample != anaSample):
				sys.stderr.write("sample_uuid mismatch: %s != %s\n" % (bioSample, anaSample))
				continue

			if (not bioDonor in donorSpecSampleBioMap.keys()):
				donorSpecSampleBioMap[bioDonor] = {}
				
			if (not bioSpec in donorSpecSampleBioMap[bioDonor].keys()):
				donorSpecSampleBioMap[bioDonor][bioSpec] = {}

			if (not bioSample in donorSpecSampleBioMap[bioDonor][bioSpec].keys()):
				donorSpecSampleBioMap[bioDonor][bioSpec][bioSample] = Set()

			donorSpecSampleBioMap[bioDonor][bioSpec][bioSample].add(json.dumps(bioObj))
			
			if (not bioDonor in donorAnaMap.keys()):
				donorAnaMap[bioDonor] = []
			
			donorAnaMap[bioDonor].append(anaObj)

	# write output
	num_files_written = writeOutput(donorSpecSampleBioMap, donorAnaMap, options.metadataOutDir)
	sys.stderr.write("%s metadata files written to %s\n" % (str(num_files_written), options.metadataOutDir))
	
	if (options.skip_upload):
		return None

	log("Now attempting to upload data.\n")

	num_files_uploaded = 0
	for dirName, subdirList, fileList in os.walk(options.metadataOutDir):
		if dirName == options.metadataOutDir:
			continue
		log('Found directory: %s\n' % dirName)
		for fileName in fileList:
			if (fileName.endswith("biospecimen.json") or fileName.endswith("analysis.json")):
				log('\t%s\n' % fileName)
				filePath = os.path.join(dirName, fileName)

				num_files_uploaded += uploadFile(filePath)

				file = open(filePath, "r")
				metadataObj = json.load(file)
				file.close()
				if ("workflow_outputs" in metadataObj.keys()) and (metadataObj["workflow_outputs"].keys() > 0):
					for dataFileName in metadataObj["workflow_outputs"].keys():
						log("dataFileName: %s\n" % (dataFileName))
						dataDir = ""
						filePath = os.path.join(dataDir, dataFileName)
						num_files_uploaded += uploadFile(filePath)

	sys.stderr.write("%s files uploaded\n" % (str(num_files_uploaded)))
	
	return None

# main program section
if __name__ == "__main__":
	main()
