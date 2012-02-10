from lxml import etree
import urllib

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
			ename = element.get("name")
			simple_type[ename] = {}
			simple_type[ename]["restriction"] = element.getchildren()[0].attrib
			elements = element.getchildren()[0].getchildren()
			simple_type[ename]["elements"] = []
			for elem in elements:
				simple_type[ename]["elements"].append(elem.get("value"))
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

	def get_matching_elements(self, element, attribute_name, attribute):
		
		if "elements" in element:
			element_list = []
			for i in range(len(element["elements"])):
				subelement = element["elements"][i]
				if "elements" in subelement:
					element_list.extend(self.get_matching_elements(subelement, attribute_name, attribute))
				else:
					element_list.append(self.get_matching_elements(subelement, attribute_name, attribute))
			return element_list
		elif attribute_name in element and element[attribute_name] == attribute and "name" in element:
			return element["name"]
		return
	
	def get_elements_of_attribute(self, attribute_name, attribute):
		
		element_list = []
	
		for i in range(len(self.schema["element"])):
			element_list.extend(self.get_matching_elements(self.schema["element"][i], attribute_name, attribute))
		
		return list(set(element_list))

	def get_elements_of_indicator(self, t):
		return "test"	


#	def get_sub_element_list(name):

#	def get_sub_schema(name):

#	def get_element_attributes(name):


if __name__ == '__main__':
	fschema = urllib.urlopen("http://election-info-standard.googlecode.com/files/vip_spec_v2.3.xsd")

	schema = schema(fschema)


	print schema.get_simpleTypes()
	print schema.get_complexTypes()
	
	print schema.get_elements_of_attribute("type", "xs:string")

#	for elem in schema.schema["element"][0]["elements"]:
#		print elem	
	#also could write a combo on the front end of get_simpleTypes() and get_elements_of_type() using each of the simple types
