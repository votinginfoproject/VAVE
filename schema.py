from lxml import etree
import urllib
import copy

INDICATORS = ["all", "sequence", "choice"]
TYPES = ["simpleType", "complexType"]
CONTENT = ["simpleContent"]

class schema:

	def __init__(self, schemafile):
		if schemafile is None:
			print "Error creating Schema: Invalid schema file used"
			return
			
		self.schema = self.create_schema(etree.parse(schemafile))
	
	def create_schema(self, schema_data):
		def getXSVal(element): #removes namespace
			return element.tag.split('}')[-1]

		def get_simple_type(element):
			simple_type = {}
			simple_type["name"] = element.get("name")
			simple_type["restriction"] = element.getchildren()[0].attrib
			elements = element.getchildren()[0].getchildren()
			simple_type["elements"] = []
			for elem in elements:
				simple_type["elements"].append(elem.get("value"))
			return simple_type	

		def get_simple_content(element):
			simple_content = {}
			simple_content["simpleContent"] = {}
			simple_content["simpleContent"]["extension"] = element.getchildren()[0].attrib
			simple_content["attributes"] = []
			attributes = element.getchildren()[0].getchildren()
			for attribute in attributes:
				simple_content["attributes"].append(attribute.attrib)
			return simple_content

		def get_elements(element):
	
	
			if len(element.getchildren()) == 0:
				return element.attrib
	
			data = {}
	
			ename = element.get("name")
			tag = getXSVal(element)

			if ename is None:
				if tag == "simpleContent":
					return get_simple_content(element)
				elif tag in INDICATORS:
					data["indicator"] = tag
				elif tag in TYPES:
					data["type"] = tag
				else:
					data["option"] = tag

			else:
				if tag == "simpleType":
					return get_simple_type(element)
				else: 
					data.update(element.attrib)
			
			data["elements"] = []
			data["attributes"] = []
			children = element.getchildren()
			
			for child in children:
				if child.get("name") is not None:
					data[getXSVal(child)+"s"].append(get_elements(child))
				else:
					data.update(get_elements(child))
			
			return data

		schema = {}
		root = schema_data.getroot()
		children = root.getchildren()
		for child in children:
			c_type = getXSVal(child)
			if child.get("name") is not None and not c_type in schema:
				schema[c_type] = []
			schema[c_type].append(get_elements(child))
		return schema

	def get_simpleTypes(self): #iterate through, return names, could combine with "complex" in separate function
		simple_types = []
		for simple in self.schema["simpleType"]:
			simple_types.append(simple.keys()[0])
		return simple_types

	def get_complexTypes(self):
		complex_types = []
		for complex_t in self.schema["complexType"]:
			complex_types.append(complex_t.keys()[0])
		return complex_types

	def matching_elements(self, element, attribute_name, attribute):
		
		element_list = []
		if attribute_name in element and element[attribute_name] == attribute and "name" in element:
			element_list.append(element["name"])
		
		if "elements" in element:
			for i in range(len(element["elements"])):
				subelement = element["elements"][i]
				element_list.extend(self.matching_elements(subelement, attribute_name, attribute))

		return element_list
	
	def get_elements_of_attribute(self, attribute_name, attribute):
		
		element_list = []
	
		for i in range(len(self.schema["element"])):
			element_list.extend(self.matching_elements(self.schema["element"][i], attribute_name, attribute))
		
		return list(set(e for e in element_list if e != None))

	def get_schema_match(self, sub_schema, name):

		if "name" in sub_schema and sub_schema["name"] == name:
			return sub_schema
		elif "elements" in sub_schema:
			for i in range(len(sub_schema["elements"])):
				new_schema = self.get_schema_match(sub_schema["elements"][i], name)
				if new_schema is not None:
					return new_schema

	def get_sub_schema(self, name): 

		type_list = ["element", "complexType", "simpleType"]

		for e_type in type_list:
			for i in range(len(self.schema[e_type])):
				new_schema = self.get_schema_match(self.schema[e_type][i], name)
				if new_schema is not None:
					return new_schema

	def get_attributes(self, attributes, name):
		if "name" in attributes and attributes["name"] == name:
			if "elements" in attributes:
				clean_attributes = copy.copy(attributes)
				clean_attributes["elements"] = len(attributes["elements"])
				return clean_attributes
			return attributes 
		elif "elements" in attributes:
			for i in range(len(attributes["elements"])):
				element_attributes = self.get_attributes(attributes["elements"][i], name)
				if element_attributes is not None:
					return element_attributes 
	
	def get_element_attributes(self, name): 

		type_list = ["element", "complexType"]
		
		for e_type in type_list:
			for i in range(len(self.schema[e_type])):
				attributes = self.get_attributes(self.schema[e_type][i], name)
				if attributes is not None:
					return attributes 

	def element_list(self, sub_schema, name):
		if "name" in sub_schema and sub_schema["name"] == name and "elements" in sub_schema:
			element_list = []
			for i in range(len(sub_schema["elements"])):
				if "name" in sub_schema["elements"][i]:
					element_list.append(sub_schema["elements"][i]["name"])
			return element_list
		elif "elements" in sub_schema:
			for i in range(len(sub_schema["elements"])):
				element_list = self.element_list(sub_schema["elements"][i], name)
				if element_list is not None:
					return element_list

	def get_element_list(self, schema_type, name):
		for i in range(len(self.schema[schema_type])):
			elements = self.element_list(self.schema[schema_type][i], name)
			if elements is not None:
				return elements

if __name__ == '__main__':
	fschema = urllib.urlopen("http://election-info-standard.googlecode.com/files/vip_spec_v2.3.xsd")

	schema = schema(fschema)

	print schema.get_simpleTypes()
	print schema.get_complexTypes()
	
	print schema.get_elements_of_attribute("indicator", "all")

	print schema.get_sub_schema("vip_id")

	print schema.get_element_attributes("source")
	print schema.get_element_attributes("feed_contact_id")

	print schema.get_element_list("element", "source")

	#print schema.schema["simpleType"]
	#print schema.schema["complexType"]

	#for elem in schema.schema["element"][0]["elements"]:
	#	print elem	
	#also could write a combo on the front end of get_simpleTypes() and get_elements_of_type() using each of the simple types
