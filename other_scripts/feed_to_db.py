from lxml import etree
import MySQLdb as mdb
import schema
import urllib

#This uses individual inserts for each new row for into the database, which is
#done due max insert size problems we could run into. Also, I think this will
#free up the database for other tasks when doing a large upload

fschema = urllib.urlopen("http://election-info-standard.googlecode.com/files/vip_spec_v3.0.xsd")
#fschema = open("schema.xsd")

schema = schema.schema(fschema)

simpleAddressTypes = schema.get_elements_of_attribute("type", "simpleAddressType")
detailAddressTypes = schema.get_elements_of_attribute("type", "detailAddressType")

ELEMENT_LIST = schema.get_element_list("element","vip_object")
SIMPLECONTENTS = {)
for elem in schema.schema["element"][0]["elements"]:
	for e in elem["elements"]:
		if "simpleContent" in e:
			if e["name"] in SIMPLECONTENTS:
				SIMPLECONTENTS[e["name"]]["parents"].append(elem['name'])
			else:
				SIMPLECONTENTS[e["name"]] = {"parents":[elem['name']]}

UNBOUNDEDS = {}
for elem in schema.schema["element"][0]["elements"]:
	for e in elem["elements"]:
		if "maxOccurs" in e and "simpleContent" not in e and e["maxOccurs"] == "unbounded":
			if e["name"] in UNBOUNDEDS:
				UNBOUNDEDS[e["name"]]["parents"].append(elem["name"])
			else:
				UNBOUNDEDS[e["name"]] = {"parents":[elem["name"]]}

fname = 'test_feed.xml'

vip_id = 32
election_id = 2110

NORMALIZED_LIST = {"locality":10000000000, "precinct":20000000000,
			"precinct_split":30000000000, "election_administration":40000000000,
			"election_official":50000000000, "polling_location":60000000000,
			"electoral_district":70000000000, "street_segment":80000000000,
			"candidate":90000000000, "ballot":110000000000,
			"contest":120000000000, "early_vote_site":130000000000,
			"referendum":140000000000, "custom_ballot":150000000000,
			"ballot_response":160000000000, "contest_result":170000000000,
			"ballot_list_result":180000000000}


xmlparser = etree.XMLParser()
connection = mdb.connect('localhost', 'vip', 'gamet1me', 'vip')
cursor = connection.cursor()

data = etree.parse(open(fname), xmlparser)
root = data.getroot()
elements = root.getchildren()

def normalized_id(element_name, element):
	if element_name == "source":
		return str(1)
	elif element_name == "election":
		return str(election_id)
	elif element_name == "state":
		return str(element.get("id"))
	else:
		return str(NORMALIZED_LIST[element_name] + int(element.get("id")))

def insert_string(element_name, element_dict):

	insert_str = "INSERT INTO " + element_name + " ("	
	insert_str += ",".join(address_dict.keys()) + ") "
	insert_str += "VALUES (\"" + "\",\"".join(address_dict.vals()) + "\")"
	return insert_str

for element in elements:

	if element.tag in ELEMENT_LIST:

		if element.tag != element_name:
			element_name = element.tag
			sub_element_list = schema.get_element_list("element",element_name)
			elem_dict = {}
		else:
			for e in elem_dict.keys():
				elem_dict[e] = ""

		sub_elements = element.getchildren()
		
		elem_dict["vip_id"] = str(vip_id)
		elem_dict["election_id"] = str(election_id)
		elem_dict["received_id"] = str(element.get("id"))
		elem_dict["is_used"] = "TRUE"
		elem_dict["normalized_id"] = normalized_id(element_name, element)
			 
		for elem in sub_elements:
			
			if element_name == "source" and elem.tag == "vip_id":
				continue
			if element_name == "contest" and elem.tag == "election_id":
				continue
			
			if elem.tag in simpleAddressTypes or elem.tag in detailAddressTypes:
				
				address_elems = elem.getchildren()
				address_dict = {}
				
				for address_elem in address_elems:
					if address_elem.text is not None:
						address_dict[address_elem] = address_elem.text.replace("\"","'")
				
				if elem.tag in simpleAddressTypes:
					insert_str = insert_string("simpleAddressType", address_dict)
				elif elem.tag in detailAddressTypes:
					insert_str = insert_string("detailAddressType", address_dict)
				
				cursor.execute(insert_str)
				elem_dict[elem.tag + "_id"] = str(cursor.lastrowid)
		
			elif elem.tag in SIMPLECONTENTS and element_name in SIMPLECONTENTS[elem.tag]["parents"]:
		
				relat_dict = {}
				relat_dict["vip_id"] = str(vip_id)
				relat_dict["election_id"] = str(election_id)
				relat_dict[element.tag + "_id"] = normalized_id(element_name, element)
				relat_dict[elem.tag] = str(NORMALIZED_LIST[elem.tag[:elem.tag.find("_id")]] + int(elem.text))

				schema_data = schema.get_element_under_parent(element.tag, elem.tag)
				for attr in schema_data["attributes"]:
					if elem.get(attr["name"]) is not None:
						relat_dict[attr["name"]] = elem.get(attr["name"])
				
				cursor.execute(insert_string(elem.tag, relat_dict))		
				
			elif "maxOccurs" in UNBOUNDEDS and element_name in UNBOUNDEDS[elem.tag]["parents"]: 
				
				relat_dict = {}
				relat_dict["vip_id"] = str(vip_id)
				relat_dict["election_id"] = str(election_id)
				relat_dict[element.tag + "_id"] = normalized_id(element_name, element)
				relat_dict[elem.tag] = str(NORMALIZED_LIST[elem.tag[:elem.tag.find("_id")]] + int(elem.text))

				cursor.execute(insert_string(elem.tag, relat_dict))

			elif elem.text is not None:
				if elem.tag.endswith("_id"):
					elem_dict[elem.tag] = str(NORMALIZED_LIST[elem.tag[:elem.tag.find("_id")]] + int(elem.text))
				else:
					elem_dict[elem.tag] = elem.text.replace("\"", "'")

		#print insert_str
		cursor.execute(insert_string(element_name, elem_dict))

connection.commit()
