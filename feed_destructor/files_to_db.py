from lxml import etree
import MySQLdb as mdb
import schema
import urllib
import os
import csv


#TODO: accomodate for sort_order as part of the relational tables
#TODO: change to github url
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

def insert_string(element_name, element_dict):

	insert_str = "INSERT INTO " + element_name + " ("	
	insert_str += ",".join(address_dict.keys()) + ") "
	insert_str += "VALUES (\"" + "\",\"".join(address_dict.vals()) + "\")"
	return insert_str

def normalized_id(element_name, received_id):
	if element_name == "source":
		return str(1)
	elif element_name == "election":
		return str(election_id)
	elif element_name == "state":
		return str(received_id)
	else:
		return str(NORMALIZED_LIST[element_name] + int(received_id))

def generate_id(element_name, row):
	return "random " #TODO:Generate IDs from files without

DIR = ""
dir_list = os.listdir(DIR)
for fname in dir_list:
	ename = fname.split(".")[0].lower() 
	if ename in ELEMENT_LIST:
	
		base_elements = []
		addresses = {}
		relationals = []
		has_id = False
	
		data = csv.DictReader(open(fname))
		headers = data.fieldnames
	
		for header in headers:
			if header == "vip_id":
				continue
			elif header == "election_id":
				continue
			elif header == "id":
				has_id = True
			elif header in ELEMENT_LIST["elements"]: #actuall need to change this to a schema call
				base_elements.append(header)
			elif header.endswith("_ids"):
				relationals.append(header)
			else:
				for simpletype in simpleAddressTypes:
					if header.startswith(simpletype):
						if simpletype not in addresses:
							addresses[simpletype] = {}
							addresses[simpletype]["type"] = "simpleAddressType"
							addresses[simpletype]["elements"] = []
						addresses[simpletype]["elements"].append(header[len(simpletype)+1:])
				for detailtype in detailAddressTypes:
					if header.startswith(detailtype):
						if detailtype not in addresses:
							addresses[detailtype] = {}
							addresses[detailtype]["type"] = "detailAddressType"
							addresses[detailtype]["elements"] = []
		
		for row in data:
			
			insert_dict = {}
			insert_dict["vip_id"] = vip_id
			insert_dict["election_id"] = election_id
			if has_id:
				insert_dict["received_id"] = row["id"]
				insert_dict["normalized_id"] = normalized_id(ename, row["id"])
			else:
				insert_dict["received_id"] = "0"
				insert_dict["normalized_id"] = generate_id(ename, row)
			
			for address in addresses:
				address_dict = {}
				for element in address["elements"]:
					address_dict[element] = row[address+"_"+element]
				cursor.execute(insert_string(address["type"], address_dict))
				insert_dict[address+"_id"] = cursor.lastrowid
				connection.commit()

			for relational in relationals:
				relat_dict = {}
				relat_dict["vip_id"] = vip_id
				relat_dict["election_id"] = election_id
				relat_dict[ename+"_id"] = insert_dict["normalized_id"]				

				relat_ids = row[relational].split(",")
				for i in range(len(relat_ids)):
					relat_dict[relational[:-1]] = normalized_id(relational[:-4],relat_ids[i])
					cursor.execute(insert_string(ename+"_"+relational[:-4],relat_dict))
					connection.commit()

			for element in base_elements:
				insert_dict[element] = row[element]

			cursor.execute(insert_string(ename, insert_dict))
			connection.commit()
					

