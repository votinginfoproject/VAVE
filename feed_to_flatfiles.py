from lxml import etree
import urllib
import schema
import os
from csv import DictWriter

fname = "large_test.xml"
SCHEMA_URL = "http://election-info-standard.googlecode.com/files/vip_spec_v"
VALID_VERSIONS = ["2.0","2.1","2.2","2.3","3.0"]

def get_fields(element_name):
	subschema = schema.get_sub_schema(element_name)
	e_list = []

	if "elements" in subschema:
		for e in subschema["elements"]:
			if e["type"] == "simpleAddressType":
				for s_e in simple_elements:
					e_list.append(e["name"] + "_" + s_e)
			elif e["type"] == "detailAddressType":
				for d_e in detail_elements:
					e_list.append(e["name"] + "_" + d_e)
			else:
				e_list.append(e["name"])
			if "simpleContent" in e:
				for sc_attrib in e["simpleContent"]["attributes"]:
					e_list.append(e["name"] + "_" + sc_attrib["name"])
	if "attributes" in subschema:
		for a in subschema["attributes"]:
			e_list.append(a["name"])

	return e_list

def clear_element(element):
	element.clear()
	while element.getprevious() is not None:
		del element.getparent()[0]
	
def extract_base_elements(context):
	for event, element in context:
		if event == "end" and element.tag in ELEMENT_LIST and len(element.getchildren()) > 0:
			yield element
			clear_element(element)


with open(fname) as xml_doc:
	
	context = etree.iterparse(xml_doc, events=("start", "end"))
	context = iter(context)
	
	event, root = context.next()
	
	version = root.attrib["schemaVersion"] 
	if version in VALID_VERSIONS:
		if version == "2.2":
			fschema = urllib.urlopen(SCHEMA_URL + version + "a.xsd")
		else:
			fschema = urllib.urlopen(SCHEMA_URL + version + ".xsd")		

	schema = schema.Schema(fschema)

	simple_elements = schema.get_element_list("complexType", "simpleAddressType")
	detail_elements = schema.get_element_list("complexType", "detailAddressType")

	ELEMENT_LIST = schema.get_element_list("element","vip_object")

	elem_fields = {}
	cur_element = ""

	for element in extract_base_elements(context):
		if element.tag != cur_element:
			
			cur_element = element.tag
		
			if os.path.exists("flat_files/" + cur_element + ".txt"):
				dw = DictWriter(open("flat_files/" + cur_element + ".txt", "a"), fieldnames=elem_fields[cur_element])
			else:
				elem_fields[cur_element] = get_fields(cur_element)
				dw = DictWriter(open("flat_files/" + cur_element + ".txt", "w"), fieldnames=elem_fields[cur_element])
				dw.writeheader()
				print elem_fields[cur_element]
				 
		element_dict = dict.fromkeys(elem_fields[cur_element], '')
		element_dict["id"] = element.get("id")
			
		sub_elements = element.getchildren()
		extras = []
		
		for elem in sub_elements:

			if elem.tag.endswith("address"):
				
				add_elems = elem.getchildren()
				
				for add_elem in add_elems:
					element_dict[elem.tag + "_" + add_elem.tag] = add_elem.text
			elif len(element_dict[elem.tag]) > 0:
				extras.append({elem.tag:{"val":elem.text, "attributes":elem.attrib}}) 
			else:
				element_dict[elem.tag] = elem.text
		
		dw.writerow(element_dict)

		for row in extras:
			e_name = row.keys()[0]
			element_dict[e_name] = row[e_name]["val"]
			if len(row[e_name]["attributes"]) > 0:
				for attr in row[e_name]["attributes"]:
					element_dict[e_name + "_" + attr] = row[e_name]["attributes"][attr]
			dw.writerow(element_dict)
			element_dict[e_name] = ""
			if len(row[e_name]["attributes"]) > 0:
				for attr in row[e_name]["attributes"]:
					element_dict[e_name + "_" + attr] = ""
		
