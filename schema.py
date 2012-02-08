from lxml import etree

INDICATORS = ["all", "sequence", "choice"]
TYPES = ["simpleType", "complexType"]
CONTENT = ["simpleContent"]

#TODO:Missing certain specific element attributes, such as min times and max times (ex. source and election have that they must be in feed once
#TODO:Required for each element. This could be generated when a check is done, or ahead of time in the schema

def getXSVal(element): #removes namespace
	return element.tag.split('}')[-1]

def base_level_doc(root):
	schema = {}
	children = root.getchildren()
	for child in children:
		c_type = getXSVal(child)
		if child.get("name") is not None and not c_type in schema:
			schema[c_type] = []
		schema[c_type].append(more_elems(child))
	return schema

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

def more_elems(element):
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
				data[getXSVal(child)].append(more_elems(child))
			else:
				data.update(more_elems(child))

	else:
		if tag == "simpleType":
			return get_simple_type(element)
		else:
			data[ename] = {}
			data[ename]["elements"] = []
			data[ename]["attributes"] = []
			children = element.getchildren()
			
			for child in children:
				if child.get("name") is not None:
					data[ename][getXSVal(child)].append(more_elems(child))
				else:
					data[ename].update(more_elems(child))
	return data

data = etree.parse(open("vip_spec_v3.0.xsd"))

root = data.getroot()

schema = base_level_doc(root)

for elem in schema["element"][0]["vip_object"]["elements"]:
	print elem
