from lxml import etree
from urllib import urlopen
from csv import DictWriter
from copy import copy
from schema import Schema
import os

SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v"
VALID_VERSIONS = ["2.1","2.2","2.3","3.0"]

class FeedToFlatFiles:
	
	def __init__(self, output_dir=os.getcwd(), schema_file=None):
		
		self.set_output_dir(output_dir)
		
		self.set_schema_props(schema_file)

	def set_output_dir(self, output_dir):
		self.output_dir = output_dir
		if not self.output_dir.endswith("/"):
			self.output_dir += "/"
		if not os.path.exists(self.output_dir):
			os.mkdir(self.output_dir)

	def set_schema_props(self, schema_file):
		
		if not schema_file:
			self.schema_version = None
			return

		schema = Schema(schema_file)
		
		self.schema_version = schema.version
		self.simple_elements = schema.get_element_list("complexType", "simpleAddressType")
		self.detail_elements = schema.get_element_list("complexType", "detailAddressType")
		self.element_list = schema.get_element_list("element","vip_object")
		self.elem_fields = self.get_fields(schema, self.element_list)

	def get_fields(self, schema, elem_list):
		
		field_list = {}
		
		for elem_name in elem_list:

			subschema = schema.get_sub_schema(elem_name)
			e_list = []

			if "elements" in subschema:
				for e in subschema["elements"]:
					if "name" not in e: 
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


	def process_feed(self, feed, output_dir=None):

		if output_dir:
			self.set_output_dir(output_dir)
		
		with open(feed) as xml_doc:

			context = etree.iterparse(xml_doc, events=("start", "end"))
			context = iter(context)

			event, root = context.next()
			version = root.attrib["schemaVersion"]
			if not self.schema_version and version in VALID_VERSIONS:
				self.set_schema_props(urlopen(SCHEMA_URL + version + ".xsd"))
			elif self.schema_version and self.schema_version != version:
				self.set_schema_props(urlopen(SCHEMA_URL + version + ".xsd"))

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
	ftff = FeedToFlatFiles("flat_files")
	ftff.process_feed("v2_1.xml")
