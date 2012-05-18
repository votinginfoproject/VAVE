import psycopg2
from httplib import HTTPConnection
from urlparse import urlparse
from schema import Schema
from urllib import urlopen
from psycopg2 import extras

SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"
REPORT_DIR = "reports/"

#TODO: Also need validation reports on insertion, since most bad rows
#will have been dropped there

def is_valid_url(url):
	
	if not url.startswith("http"):
		url = "http://" + url
	
	host, path = urlparse(url)[1:3]
	try:
		conn = HTTPConnection(host)
		conn.request('HEAD', path)
		if conn.getresponse().status == 200:
			return True
	except:
		return False
	return False

def street_segment_checks():
	print "random"

def missing_requireds(requireds):
		
	for xml_type in requireds:
		print xml_type
		required_query = "SELECT * FROM {0} WHERE {1} IS NULL"
		if xml_type == "xs:integer":
			required_query += " OR {1} = 0"
		if xml_type == "xs:string":
			required_query += " OR {1} LIKE ''"
		
		for i in range(len(requireds[xml_type])):
			query = required_query.format(requireds[xml_type][i]["table"], requireds[xml_type][i]["element"])
			dict_cur.execute(query)
			for c in dict_cur:
				print c

def type_check(types):

	for xml_type in types:
		print xml_type
		for i in range(len(types[xml_type])):
			element = types[xml_type][i]["element"]
			type_query = "SELECT feed_id, {0} FROM {1}".format(element, types[xml_type][i]["table"])
			dict_cur.execute(type_query)
			for c in dict_cur:
				if not c[element]:
					continue
				if xml_type == "xs:integer":
					try:
						int(c[element])
					except:
						print c
				elif xml_type == "xs:string" and element.find("<") >= 0: #need to add a series of invalid characters here
					print "invalid characters: " + str(c)

schema = Schema(urlopen(SCHEMA_URL))
connection = psycopg2.connect(host="localhost", database="vip", user="jensen", password="gamet1me")
dict_cur = connection.cursor(cursor_factory=extras.RealDictCursor)
vip_id = 23
election_id = 1000


element_list = schema.get_element_list("element","vip_object")
simple_elements = schema.get_sub_schema("simpleAddressType")
detail_elements = schema.get_sub_schema("detailAddressType")

requireds = {"xs:integer":[],"xs:string":[],"xs:date":[],"xs:dateTime":[],"oebEnum":[],"yesNoEnum":[]}
types = {"xs:integer":[],"xs:string":[],"xs:date":[],"xs:dateTime":[],"oebEnum":[],"yesNoEnum":[]} 

for root_elem in element_list:
	subschema = schema.get_sub_schema(root_elem)
	for element in subschema["elements"]:
		if "maxOccurs" in element and element["maxOccurs"] == "unbounded":
			continue
		if element["type"] == "simpleAddressType":
			for s_e in simple_elements["elements"]:
				if ("minOccurs" not in element or int(element["minOccurs"]) > 0) and ("minOccurs" not in s_e or int(s_e["minOccurs"]) > 0): 
					requireds[s_e["type"]].append({"element":element["name"] + "_" + s_e["name"], "table":root_elem})
				types[s_e["type"]].append({"element":element["name"] + "_" + s_e["name"], "table":root_elem})
		elif element["type"] == "detailAddressType":
			for d_e in detail_elements["elements"]:
				if ("minOccurs" not in element or int(element["minOccurs"]) > 0) and ("minOccurs" not in d_e or int(d_e["minOccurs"]) > 0):
					requireds[d_e["type"]].append({"element":element["name"] + "_" + d_e["name"], "table":root_elem})
				types[d_e["type"]].append({"element":element["name"] + "_" + d_e["name"], "table":root_elem})
		elif "simpleContent" not in element:
			if "minOccurs" not in element or int(element["minOccurs"]) > 0:
				requireds[element["type"]].append({"element":element["name"], "table":root_elem})
			types[element["type"]].append({"element":element["name"], "table":root_elem})

missing_requireds(requireds)

type_check(types)
#print types
#print requireds
