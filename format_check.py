from os import listdir
from schema import schema
import csv
import ConfigParser

#TODO: Try/catch around the config calls, just in case file_name and header are missing from a section

ADDRESS_TYPES = ["simpleAddressType", "detailAddressType"]
REQUIRED_ELEMENTS = ["source", "election", "state", "locality", "precinct", "polling_location", "street_segment"]
CONFIG_FNAME = "vip.cfg"

class format_check:

	def __init__(self, schema_file, directory=""):
		
		self.schema = schema(schema_file)
		self.element_list = self.schema.get_element_list("element","vip_object")
		self.addresses = self.address_data()
		if not directory.endswith("/"): #consistency for later file lookups
			directory += "/"
		self.directory = directory
		self.dir_list = listdir(directory)
		self.invalid_files = []
		self.missing_files = []
		self.valid_files = {}
		self.invalid_columns = {}
		self.missing_columns = {}
		self.valid_columns = {}
		self.invalid_sections = []
		if self.uses_config():
			self.validate_with_config()
		else:
			self.validate()

	def address_data(self):

		addresses = {}

		for atype in ADDRESS_TYPES:
			for element in self.schema.get_elements_of_attribute("type", atype):
				addresses[element] = {}
				addresses[element]["type"] = atype
				addresses[element]["elements"] = self.schema.get_element_list("complexType", atype)
		return addresses

	def column_check(self, ename, fname, header):
		
		invalid_columns = []
		valid_columns = []
		elem_schema = self.schema.get_sub_schema(ename)
		element_list = []
		required_list = []
		for element in elem_schema["elements"]:
			if element["type"] in self.schema.get_complexTypes():
				for address_element in self.schema.get_element_list("complexType", element["type"]):
					element_list.append(element["name"]+"_"+address_element)
			else:
				element_list.append(element["name"])
				if "simpleContent" in element:
					for attrib in element["attributes"]:
						element_list.append(element["name"]+"_"+attrib["name"])
				if "minOccurs" not in element or int(element["minOccurs"]) > 0:
					required_list.append(element["name"])
		for element in elem_schema["attributes"]:
			element_list.append(element["name"])

		for column in header:
			
			if column in element_list:
				valid_columns.append(column)
			else:
				invalid_columns.append(column)
	
		if len(invalid_columns) > 0:
			self.invalid_columns[ename] = {"file_name":fname, "elements":invalid_columns}
		if len(valid_columns) > 0:
			self.valid_columns[ename] = {"file_name":fname, "elements":valid_columns}
		for elem in required_list:
			if elem not in valid_columns:
				if ename not in self.missing_columns:
					self.missing_columns[ename] = {"file_name":fname, "elements":[]}
				self.missing_columns[ename]["elements"].append(elem)

	def file_list_check(self, valid_files):

		missing_files = []
		
		for f in REQUIRED_ELEMENTS:
			if f not in valid_files:
				missing_files.append(f)

		return missing_files		

	def validate_with_config(self):
		print "validating with config"

		config = ConfigParser.ConfigParser()
		config.read(self.directory + CONFIG_FNAME)
		sections = config.sections()

		for section in sections:

			if section not in self.element_list:
				self.invalid_sections.append(section)
				continue
			
			fname = config.get(section, "file_name")

			if fname not in self.dir_list:
				self.invalid_files.append(fname)
				continue

			fieldnames = config.get(section, "header").split(",")

			with open(self.directory+fname) as f:

				data = csv.reader(f)

				if len(data.next()) != len(fieldnames):
					self.invalid_files.append(fname)

				self.column_check(section, fname, fieldnames)

			self.valid_files[fname] = section 

		self.missing_files = self.file_list_check(sections)			

	def validate(self):

		print "validate w/o config"

		for fname in self.dir_list:
			
			ename = fname.split(".")[0].lower()
			
			if ename in self.element_list:
				
				self.valid_files[fname] = ename

				with open(self.directory+fname) as f:
				
					data = csv.DictReader(f)
	
					self.column_check(ename, fname, data.fieldnames)

			else:
				
				self.invalid_files.append(fname)	

		self.missing_files = self.file_list_check(self.valid_files.values())

	def uses_config(self):
		
		if CONFIG_FNAME in self.dir_list:
			return True
		return False

	def get_valid_files(self):
		return self.valid_files.keys()
	
	def get_invalid_files(self):
		return self.invalid_files
	
	def get_missing_files(self):
		return self.missing_files

	def get_invalid_sections(self):
		
		if self.uses_config():
			return self.invalid_sections
		return None
	
	def get_valid_columns(self):
		return self.valid_columns

	def get_missing_columns(self):
		return self.missing_columns

	def get_invalid_columns(self):
		return self.invalid_columns	

if __name__ == '__main__':
	import urllib
	
	fschema = urllib.urlopen("http://election-info-standard.googlecode.com/files/vip_spec_v3.0.xsd")

	fc = format_check(fschema, "../demo_data/format_check")

	print "valid files: " + str(fc.get_valid_files())
	print "invalid files: " + str(fc.get_invalid_files())
	print "missing files: " + str(fc.get_missing_files())
	print "invalid sections: " + str(fc.get_invalid_sections())
	print "valid columns: " + str(fc.get_valid_columns())
	print "invalid columns: " + str(fc.get_invalid_columns())
	print "missing columns: " + str(fc.get_missing_columns())
	
