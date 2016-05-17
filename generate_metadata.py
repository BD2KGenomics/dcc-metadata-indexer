from pprint import pprint
from jsonmerge import merge
import json
import csv

class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

# open the file as a tsv dictionary
# make a nested data structure per donor for biospecimen
# dump the data structure
# for each line, save key=project, program, donor and value = workflow -> files, workflow -> parent
# dump that structure
# use the biospecimen hash to make biospecimen.json
# use the worklfows to make analysis.json

# data
data = AutoVivification()
files = AutoVivification()

with open('sample.tsv') as tsvin:
    tsvin = csv.DictReader(tsvin, delimiter='\t')
    for row in tsvin:
        data[row['Program']][row['Project']][row['Center Name']][row['Submitter Donor ID']][row['Submitter Specimen ID']][row['Submitter Specimen Type']] = row['Submitter Sample ID']
        files[ row['Program'] + " " + row['Project'] + " " + row['Center Name'] + " " + row['Submitter Donor ID'] ]['files'][row['Analysis Type']][row['File Path']] = row['File Type']


pprint(data)
pprint(files)


