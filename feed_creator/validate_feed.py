import psycopg2
from httplib import HTTPConnection
from urlparse import urlparse
from schema import Schema
from urllib import urlopen
from psycopg2 import extras

SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"
REPORT_DIR = "reports/"

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

def missing_requireds(r_dict):
	
	required_query = "SELECT * FROM {0} WHERE {1} IS NULL"# OR {1} LIKE '0'"

	for element in r_dict:
		print element
		for i in range(len(r_dict[element])):
			sub_elem = r_dict[element][i]
			query = required_query.format(element, sub_elem)
			dict_cur.execute(query)
			for c in dict_cur:
				print c

schema = Schema(urlopen(SCHEMA_URL))
connection = psycopg2.connect(host="localhost", database="vip", user="username", password="password")
dict_cur = connection.cursor(cursor_factory=extras.RealDictCursor)
vip_id = 23
election_id = 1000


element_list = schema.get_element_list("element","vip_object")
simple_elements = schema.get_sub_schema("simpleAddressType")
detail_elements = schema.get_sub_schema("detailAddressType")

required_dict = {}

for root_elem in element_list:
	subschema = schema.get_sub_schema(root_elem)
	for element in subschema["elements"]:
		if "minOccurs" not in element or int(element["minOccurs"]) > 0:
			if root_elem not in required_dict:
				required_dict[root_elem] = []
			if element["type"] == "simpleAddressType":
				for s_e in simple_elements["elements"]:
					if "minOccurs" not in s_e or int(s_e["minOccurs"]) > 0: 
						required_dict[root_elem].append(element["name"] + "_" + s_e["name"])
			elif element["type"] == "detailAddressType":
				for d_e in detail_elements["elements"]:
					if "minOccurs" not in d_e or int(d_e["minOccurs"]) > 0:
						required_dict[root_elem].append(element["name"] + "_" + d_e["name"])
			elif "simpleContent" not in element:	
				required_dict[root_elem].append(element["name"])

missing_requireds(required_dict)

street_segment_checks()
