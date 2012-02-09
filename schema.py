from lxml import etree
import urllib

INDICATORS = ["all", "sequence", "choice"]
TYPES = ["simpleType", "complexType"]
CONTENT = ["simpleContent"]

#TODO:Required for each element. This could be generated when a check is done, or ahead of time in the schema
#maybe we do not need to do required, have a function that will return the required elements for an element based on
#on the parameters (ex. no minOccurs, minOccurs = 1, etc.)
#difference with 'any' elements
#maybe return 'database' with element and all sub-elements info

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
			
				data["elements"] = []
				data["attributes"] = []
		
				children = element.getchildren()
				for child in children:
					if child.get("name") is not None:
						data[getXSVal(child)+"s"].append(get_elements(child))
					else:
						data.update(get_elements(child))

			else:
				if tag == "simpleType":
					return get_simple_type(element)
				else:
					data[ename] = {}
					data[ename].update(element.attrib)
					del data[ename]["name"]
					data[ename]["elements"] = []
					data[ename]["attributes"] = []
					children = element.getchildren()
			
					for child in children:
						if child.get("name") is not None:
							data[ename][getXSVal(child)+"s"].append(get_elements(child))
						else:
							data[ename].update(get_elements(child))
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


if __name__ == '__main__':
	fschema = urllib.urlopen("http://election-info-standard.googlecode.com/files/vip_spec_v2.3.xsd")

	schema = schema(fschema)

	print schema

	for elem in schema.schema["element"][0]["vip_object"]["elements"]:
		print elem
