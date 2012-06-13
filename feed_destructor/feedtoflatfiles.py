from lxml import etree
from csv import DictWriter
from os import path
from copy import copy

def clear_element(element):
	element.clear()
	while element.getprevious() is not None:
		del element.getparent()[0]

def extract_base_elements(context, element_list):
	for event, element in context:
		if event == "end" and element.tag in element_list and len(element.getchildren()) > 0:
			yield element
			clear_element(element)

def file_writer(directory, e_name, fields):

	output_file = directory + e_name + ".txt"
		
	if path.exists(output_file):
		return DictWriter(open(output_file, "a"), fieldnames=fields)
	else:
		w = DictWriter(open(output_file, "w"), fieldnames=fields)
		w.writeheader()
		return w

def element_extras(extras, elem_dict):
		
	for row in extras:
			
		temp_dict = copy(elem_dict)
			
		e_name = row.keys()[0]
		temp_dict[e_name] = row[e_name]["val"]
			
		if len(row[e_name]["attributes"]) > 0:
			for attr in row[e_name]["attributes"]:
				temp_dict[e_name + "_" + attr] = row[e_name]["attributes"][attr]
			
		yield temp_dict

def process_sub_elems(elem, elem_fields):
	elem_dict = dict.fromkeys(elem_fields, '')
	elem_dict["id"] = elem.get("id")
			
	sub_elems = elem.getchildren()
	extras = []
		
	for sub_elem in sub_elems:

		if sub_elem.tag.endswith("address"):
				
			add_elems = sub_elem.getchildren()
			
			for add_elem in add_elems:
				elem_dict[sub_elem.tag + "_" + add_elem.tag] = add_elem.text
		
		elif len(elem_dict[sub_elem.tag]) > 0:
			extras.append({sub_elem.tag:{"val":sub_elem.text, "attributes":sub_elem.attrib}}) 
		else:
			elem_dict[sub_elem.tag] = sub_elem.text

	return elem_dict, extras

def process_db_sub_elems(elem, elem_fields):
	elem_dict = dict.fromkeys(elem_fields[elem.tag], '')
	elem_dict["id"] = elem.get("id")

	sub_elems = elem.getchildren()
	extras = [] 

	for sub_elem in sub_elems:
		sub_name = sub_elem.tag
		if sub_name.endswith("address"):
			add_elems = sub_elem.getchildren()
			for add_elem in add_elems:
				elem_dict[sub_name + "_" + add_elem.tag] = add_elem.text
		elif sub_name not in elem_dict:
			table_name = elem.tag + "_" + sub_name[:sub_name.find("_id")]
			extra = {"table":table_name, "elements":dict.fromkeys(elem_fields[table_name])}
			extra["elements"][elem.tag + "_id"] = elem.get("id")
			extra["elements"][sub_name] = sub_elem.text
			attributes = sub_elem.attrib
			for a in attributes:
				extra["elements"][a] = attributes[a]
			extras.append(extra)
		else:
			elem_dict[sub_name] = sub_elem.text
	return elem_dict, extras

def feed_to_element_files(output_directory, feed_file, element_props, version):
	with open(feed_file) as xml_doc:
		context = etree.iterparse(xml_doc, events=("start", "end"))
		context = iter(context)

		event, root = context.next()
		feed_version = root.attrib["schemaVersion"]
		if feed_version != version:
			print "version error"
		
		e_name = ""

		for elem in extract_base_elements(context, element_props.keys()):
			if elem.tag != e_name:
			
				e_name = elem.tag
				writer = file_writer(output_directory, e_name, element_props[e_name])
		
			elem_dict, extras = process_sub_elems(elem, element_props[e_name])
		
			writer.writerow(elem_dict)

			for row_dict in element_extras(extras, elem_dict):
				writer.writerow(row_dict)	

def feed_to_db_files(directory, feed_file, db_props, version):
	with open(feed_file) as xml_doc:
		context = etree.iterparse(xml_doc, events=("start", "end"))
		context = iter(context)

		event, root = context.next()
		feed_version = root.attrib["schemaVersion"]
		if feed_version != version:
			print "version error"
		e_name = ""
		for elem in extract_base_elements(context, db_props.keys()):
			if elem.tag != e_name:
			
				e_name = elem.tag
				writer = file_writer(directory, e_name, db_props[e_name])
		
			elem_dict, extras = process_db_sub_elems(elem, db_props)
		
			writer.writerow(elem_dict)

			for extra in extras:
				temp_writer = file_writer(directory, extra["table"], db_props[extra["table"]])
				temp_writer.writerow(extra["elements"])
