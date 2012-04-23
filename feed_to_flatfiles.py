#This works with one caveat, their can't be multiples of a specific value in a specific element, a problem for another time
#that we'll need to work on the database schema for
#one option: multiple exports for flat files for the elements with multiple sub elements of the same type, fix was pulled
#into or out of the database into the feed

from sys import argv
from csv import DictWriter
from lxml import etree
from os.path import exists
import schema
import urllib

fschema = exists('schema.xsd') and open('schema.xsd') \
    or urllib.urlopen("http://election-info-standard.googlecode.com/files/vip_spec_v2.3.xsd")

schema = schema.schema(fschema)

simpleAddressTypes = schema.get_elements_of_attribute("type", "simpleAddressType")
detailAddressTypes = schema.get_elements_of_attribute("type", "detailAddressType")

ELEMENT_LIST = schema.get_element_list("element","vip_object")
fname = len(argv) >= 1 and argv[1] or 'test_feed.xml'

xmlparser = etree.XMLParser()
data = etree.parse(open(fname), xmlparser)
root = data.getroot()
elements = root.getchildren()
element_name = ""
sub_element_list = []
write_list = []
w, d = None, None

for element in elements:

	if element.tag in ELEMENT_LIST:

		if element.tag != element_name:
			element_name = element.tag
			if w is not None:
				w.close()
			print "writing " + element_name + " elements"
			write_list = ["id"]
			sub_element_list = schema.get_element_list("element",element_name)	
			for e in sub_element_list:
				if e in simpleAddressTypes:
					for s in schema.get_element_list("complexType","simpleAddressType"):
						write_list.append(e + "_" + s)
				if e in detailAddressTypes:
					for s in schema.get_element_list("complexType","detailAddressType"):
						write_list.append(e + "_" + s)
				else:
					write_list.append(e)

			# create an output CSV file and write header row.
			w = open(element_name + ".txt", "w")
			d = DictWriter(w, write_list)
			d.writerow(dict( [(col, col) for col in d.fieldnames] ))
			
		element_dict = {}
		element_dict["id"] = element.get("id")
			
		sub_elements = element.getchildren()
		
		for elem in sub_elements:
			
			if elem.tag in simpleAddressTypes or elem.tag in detailAddressTypes:
				add_elems = elem.getchildren()
				
				for add_elem in add_elems:
					element_dict[elem.tag + "_" + add_elem.tag] = add_elem.text
			else:
				element_dict[elem.tag] = elem.text
		
        # write a single data row to output CSV.
		row = dict( [(col, element_dict.get(col, None)) for col in d.fieldnames] )
		d.writerow(row)
