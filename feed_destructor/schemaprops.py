from schema import Schema
from urllib import urlopen
from os import path

SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v"
VALID_VERSIONS = ["3.0", "2.3", "2.2", "2.1"]
DEFAULT_VERSION = "3.0"

class SchemaProps:

	def __init__(self, schema_data=None):
		if not schema_data:
			self.schema = Schema(urlopen(SCHEMA_URL + DEFAULT_VERSION + ".xsd"))
		elif schema_data in VALID_VERSIONS:
			self.schema = Schema(urlopen(SCHEMA_URL + schema_data + ".xsd"))
		elif path.exists(schema_data):
			self.schema = Schema(open(schema_data))
		else:
			self.schema = Schema(urlopen(SCHEMA_URL + DEFAULT_VERSION + ".xsd"))

		self.version = self.schema.version
		self.simple_elements = self.schema.get_element_list("complexType", "simpleAddressType")
		self.detail_elements = self.schema.get_element_list("complexType", "detailAddressType")
		self.element_list = self.schema.get_element_list("element","vip_object")

		self.create_headers()

	def address_fields(self, e_name, e_type):
		e_list = []
		for e in self.schema.get_element_list("complexType", e_type):
			e_list.append(e_name + "_" + e)
		return e_list

	def create_headers(self):

		self.element_header = {}
		self.db_header = {}
		self.conversion_data = {}
		
		for elem_name in self.element_list:

			subschema = self.schema.get_sub_schema(elem_name)
			elem_list = []
			db_list = []
			conversion_list = {elem_name:{}}

			if "elements" in subschema:
				for e in subschema["elements"]:
					e_name = e["name"]
					if e["type"].endswith("AddressType"):
						temp_list = self.address_fields(e_name, e["type"])
						elem_list.extend(temp_list)
						db_list.extend(temp_list)
						for t in temp_list:
							conversion_list[elem_name][t] = t
					elif "simpleContent" in e:
						elem_list.append(e_name)
						sc_name = elem_name + "_" + e_name[:e_name.find("_id")]
						conversion_list[sc_name] = {"id":elem_name + "_id", e_name:e_name}
						temp_list = [elem_name + "_id", e_name]				 
						for sc_attr in e["simpleContent"]["attributes"]:
							elem_list.append(e_name + "_" + sc_attr["name"])
							temp_list.append(sc_attr["name"])
							conversion_list[sc_name][e_name + "_" + sc_attr["name"]] = sc_attr["name"]
						self.db_header[sc_name] = temp_list
					elif "maxOccurs" in e and e["maxOccurs"] == "unbounded":
						elem_list.append(e_name)
						unbounded_name = elem_name + "_" + e_name[:e_name.find("_id")]
						self.db_header[unbounded_name] = [elem_name + "_id", e_name]
						conversion_list[unbounded_name] = {"id":elem_name + "_id", e_name:e_name}
					else:
						elem_list.append(e_name)
						db_list.append(e_name)
						conversion_list[elem_name][e_name] = e_name
			if "attributes" in subschema:
				for a in subschema["attributes"]:
					elem_list.append(a["name"])
					db_list.append(a["name"])
					conversion_list[elem_name][a["name"]] = a["name"]
			self.element_header[elem_name] = elem_list
			self.db_header[elem_name] = db_list
			self.conversion_data[elem_name] = conversion_list
	
	def header(self, file_format, element):
		if file_format == "db":
			return self.db_header[element]
		elif file_format == "element":
			return self.element_header[element]

	def full_header_data(self, file_format):
		if file_format == "db":
			return self.db_header
		elif file_format == "element":
			return self.element_header

	def key_list(self, file_format):
		if file_format == "db":
			return self.db_header.keys()
		elif file_format == "element":
			return self.element_header.keys()

	def get_conversion_data(self):
		return self.conversion_data

	def conversion_by_element(self, element):
		return self.conversion_data[element]

if __name__ == "__main__":
	sp = SchemaProps()
	print sp.header("db", "source")
	print sp.key_list("db")
	print sp.key_list("element")
	print sp.header("db", "precinct")
	print sp.header("element", "precinct")
	print sp.get_conversion_data()
