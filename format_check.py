import os
import schema
import urllib
import csv

ADDRESS_TYPES = ["simpleAddressType", "detailAddressType"]
DIR = "../demo_data/format_check"
REQUIRED_ELEMENTS = ["source", "election", "state", "locality", "precinct", "polling_location", "street_segment"]

fschema = urllib.urlopen("http://election-info-standard.googlecode.com/files/vip_spec_v3.0.xsd")
schema = schema.schema(fschema)
ELEMENT_LIST = schema.get_element_list("element", "vip_object")
addresses = {}
use_config = False
report = {"invalid_files":[], "valid_elements":[], "missing_files":[], "invalid_columns":{}}

for atype in ADDRESS_TYPES:
	for element in schema.get_elements_of_attribute("type", atype):
		addresses[element] = {}
		addresses[element]["type"] = atype
		addresses[element]["elements"] = schema.get_element_list("complexType", atype)

def get_bad_columns(ename, header):

	bad_columns = []

	for column in header:
	
		is_valid = False
		if column == "id":
			is_valid = True
		elif column in schema.get_element_list("element", ename):
			is_valid = True
		elif column.endswith("_ids") and column[:-1] in schema.get_element_list("element", ename):
			is_valid = True
		else:
			for address in addresses:
				if ename.startswith(address):
					if ename[len(address)+1:] in address["elements"]:
						is_valid = True
					break
		if is_valid is False:
			bad_columns.append(column)
	
	return bad_columns

dir_list = os.listdir(DIR) #directory file list
if "vip.cfg" in dir_list:
	import argparse
	use_config = True

if not use_config:

	for fname in dir_list:

		ename = fname.split(".")[0].lower()
				
		if ename in ELEMENT_LIST:
			
			report["valid_elements"].append(ename)
			
			data = csv.DictReader(open(DIR+"/"+fname))
			header = data.fieldnames

			bad_columns = get_bad_columns(ename, header)
		else:
			report["invalid_files"].append(fname)
			
		if len(bad_columns) > 0:
			report["invalid_columns"][ename] = bad_columns

for f in REQUIRED_ELEMENTS:
	if f not in report["valid_elements"]:
		if "missing_requireds" not in report:
			report["missing_requireds"] = []
		report["missing_requireds"].append(f)
print addresses

print report
