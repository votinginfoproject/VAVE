#This works with one caveat, their can't be multiples of a specific value in a specific element, a problem for another time
#that we'll need to work on the database schema for
#one option: multiple exports for flat files for the elements with multiple sub elements of the same type, fix was pulled
#into or out of the database into the feed

from lxml import etree
import schema
import urllib

#fschema = urllib.urlopen("http://election-info-standard.googlecode.com/files/vip_spec_v2.3.xsd")
fschema = open("schema.xsd")

schema = schema.schema(fschema)

simpleAddressTypes = schema.get_elements_of_attribute("type", "simpleAddressType")
detailAddressTypes = schema.get_elements_of_attribute("type", "detailAddressType")

ELEMENT_LIST = schema.get_element_list("element","vip_object")
fname = 'test_feed.xml'

xmlparser = etree.XMLParser()
data = etree.parse(open(fname), xmlparser)
root = data.getroot()
elements = root.getchildren()
element_name = ""
sub_element_list = []
write_list = []
w = None

for element in elements:

	if element.tag in ELEMENT_LIST:

		if element.tag != element_name:
			element_name = element.tag
			if w is not None:
				w.close()
			w = open(element_name + ".txt", "w")
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
			w.write(",".join(write_list) + "\n") #write column headers
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
		write_string = ""
		for wr in write_list:
			
			write_string += '"'
			
			if wr in element_dict and element_dict[wr] is not None:
				write_string += element_dict[wr].replace('"', "'")
			write_string += '",'
		write_string = write_string[:-1] + "\n"
		w.write(write_string)

