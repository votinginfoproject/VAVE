from lxml import etree
from urllib import urlopen
from csv import DictWriter
from copy import copy
from schema import Schema
import os

SCHEMA_URL = "http://election-info-standard.googlecode.com/files/vip_spec_v"
VALID_VERSIONS = ["2.0","2.1","2.2","2.3","3.0"]

class FeedToFlatFiles:
	
	def __init__(self, feed, output_dir="", schema_file=None):
		
		self.output_dir = output_dir
		if len(self.output_dir) > 0 and not self.output_dir.endswith("/"):
			self.output_dir += "/"
		 
		if not os.path.isdir(self.output_dir):
			os.mkdir(self.output_dir)
		
		if schema_file:
			self.schema = Schema(schema_file)
		else:
			self.schema = Schema(self.get_schema(feed))

		self.simple_elements = self.schema.get_element_list("complexType", "simpleAddressType")
		self.detail_elements = self.schema.get_element_list("complexType", "detailAddressType")
		self.element_list = self.schema.get_element_list("element","vip_object")
		self.elem_fields = self.get_fields(self.element_list)

		self.process_feed(feed)

	def get_schema(self, feed):
		with open(feed) as xml_doc:
			context = etree.iterparse(xml_doc, events=("start","end"))
			context = iter(context)
	
			event, root = context.next()

			version = root.attrib["schemaVersion"] 

			if version in VALID_VERSIONS and version == "2.2":
				return urlopen(SCHEMA_URL + version + "a.xsd")
			elif version in VALID_VERSIONS:
				return urlopen(SCHEMA_URL + version + ".xsd")		
			return

	def file_writer(self, e_name):

		output_file = self.output_dir + e_name + ".txt"
		
		if os.path.exists(self.output_dir + e_name + ".txt"):
			return DictWriter(open(output_file, "a"), fieldnames=self.elem_fields[e_name])
		else:
			w = DictWriter(open(output_file, "w"), fieldnames=self.elem_fields[e_name])
			w.writeheader()
			return w

	def process_sub_elems(self, elem):
		
		elem_dict = dict.fromkeys(self.elem_fields[elem.tag], '')
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


	def process_feed(self, feed):
		
		with open(feed) as xml_doc:

			context = etree.iterparse(xml_doc, events=("start", "end"))
			context = iter(context)
			context.next()		

			e_name = ""

			for elem in self.extract_base_elements(context):
				if elem.tag != e_name:
			
					e_name = elem.tag
					writer = self.file_writer(e_name)
		
				elem_dict, extras = self.process_sub_elems(elem)
		
				writer.writerow(elem_dict)

				for row_dict in self.extra_rows(extras, elem_dict):
					writer.writerow(row_dict)

	def extra_rows(self, extras, elem_dict):
		
		for row in extras:
			
			temp_dict = copy(elem_dict)
			
			e_name = row.keys()[0]
			temp_dict[e_name] = row[e_name]["val"]
			
			if len(row[e_name]["attributes"]) > 0:
				for attr in row[e_name]["attributes"]:
					temp_dict[e_name + "_" + attr] = row[e_name]["attributes"][attr]
			
			yield temp_dict
			
	def get_fields(self, elem_list):
		
		field_list = {}
		
		for elem_name in elem_list:

			subschema = self.schema.get_sub_schema(elem_name)
			e_list = []

			if "elements" in subschema:
				for e in subschema["elements"]:
					if "name" not in e: #issue with schema version 2.3 that has no name for in simple content
						continue
					e_name = e["name"]
					if e["type"] == "simpleAddressType":
						for s_e in self.simple_elements:
							e_list.append(e_name + "_" + s_e)
					elif e["type"] == "detailAddressType":
						for d_e in self.detail_elements:
							e_list.append(e_name + "_" + d_e)
					else:
						e_list.append(e_name)
					if "simpleContent" in e:
						for sc_attr in e["simpleContent"]["attributes"]:
							e_list.append(e_name + "_" + sc_attr["name"])
			if "attributes" in subschema:
				for a in subschema["attributes"]:
					e_list.append(a["name"])
			
			field_list[elem_name] = []
			field_list[elem_name] = e_list

		return field_list

	def clear_element(self, element):
		element.clear()
		while element.getprevious() is not None:
			del element.getparent()[0]
	
	def extract_base_elements(self, context):
		for event, element in context:
			if event == "end" and element.tag in self.element_list and len(element.getchildren()) > 0:
				yield element
				self.clear_element(element)

if __name__ == '__main__':
	test = FeedToFlatFiles('v3_0.xml', "flat_files")
