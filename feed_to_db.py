from lxml import etree
import MySQLdb as mdb
import schema
import urllib

#This uses individual inserts for each new row for into the database, which is
#done due max insert size problems we could run into. Also, I think this will
#free up the database for other tasks when doing a large upload

#fschema = urllib.urlopen("http://election-info-standard.googlecode.com/files/vip_spec_v2.3.xsd")
fschema = open("schema.xsd")

schema = schema.schema(fschema)

simpleAddressTypes = schema.get_elements_of_attribute("type", "simpleAddressType")
detailAddressTypes = schema.get_elements_of_attribute("type", "detailAddressType")

ELEMENT_LIST = schema.get_element_list("element","vip_object")
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
element_name = ""
sub_element_list = []

for element in elements:

	if element.tag in ELEMENT_LIST:

		if element.tag != element_name:
			element_name = element.tag
			sub_element_list = schema.get_element_list("element",element_name)

		sub_elements = element.getchildren()
		
		insert_str = 'INSERT INTO ' + element.tag + ' (received_id, is_used, normalized_id,vip_id,election_id'
		
		val_str = ') VALUES (' + str(element.get('id')) + ',TRUE'
		if element_name == "source":
			val_str += ',1'
		elif element_name == "election":
			val_str += ',' + str(election_id)
		elif element_name == "state":
			val_str += ',' + str(element.get('id'))
		else:
			val_str += ',' + str(NORMALIZED_LIST[element_name] + int(element.get('id')))
		val_str += ',' + str(vip_id)
		val_str += ',' + str(election_id)
		 
		for elem in sub_elements:
			
			if element_name == "source" and elem.tag == "vip_id":
				continue
			if element_name == "contest" and elem.tag == "election_id":
				continue
			
			if elem.tag in simpleAddressTypes or elem.tag in detailAddressTypes:
				add_elems = elem.getchildren()
				insert_str += ', ' + elem.tag + '_id'
				if elem.tag in simpleAddressTypes:
					add_insert_str = 'INSERT INTO simpleAddressType ('
				elif elem.tag in detailAddressTypes:
					add_insert_str = 'INSERT INTO detailAddressType ('
				add_val_str = ') VALUES ('
				for add_elem in add_elems:
					add_insert_str += add_elem.tag + ','
					if add_elem.text is None:
						add_val_str += '\"\",'
					else:
						add_val_str += '\"' + add_elem.text.replace('"', "'") + '\",'
				add_insert_str = add_insert_str[:add_insert_str.rfind(',')] + add_val_str[:add_val_str.rfind(',')] + ')'	
				cursor.execute(add_insert_str)
				val_str += ', \"' + str(cursor.lastrowid) + '\"'
			else:
				#first pass: this is written super naively, it will do a check on every single element
				#as it is inserted, need to store the info locally here to prevent that
				#could also create a list ahead of time of values for relational tables
				schema_data = schema.get_element_under_parent(element.tag, elem.tag)
				if "simpleContent" in schema_data:
					relat_insert_str = 'INSERT INTO ' + element.tag + '_' + elem.tag[:elem.tag.find("_id")] + ' ('
					relat_insert_str += 'vip_id,election_id'
					relat_insert_str += ',' + element.tag + '_id,' + elem.tag
					relat_value_str = ') VALUES (' + vip_id + ',' + election_id
					relat_value_str += ',' + str(NORMALIZED_LIST[element_name] + int(element.get('id')))
					relat_value_str += ',' + str(NORMALIZED_LIST[elem.tag[:elem.tag.find("_id")]] + int(element.text))
					for attr in schema_data["attributes"]:
						if elem.get(attr["name"]) is not None:
							relat_insert_str += ',' + attr["name"]
							relat_value_str += ',' + elem.get(attr["name"])
					relat_insert_str += relat_value_str + ')'
					cursor.execute(relat_insert_str)		
				elif "maxOccurs" in schema_data and schema_data["maxOccurs"] = "unbounded":
					relat_insert_str = 'INSERT INTO ' + element.tag + '_' + elem.tag[:elem.tag.find("_id")] + ' ('
					relat_insert_str += 'vip_id,election_id,' + element.tag + '_id,' + elem.tag + ')'
					relat_insert_str += ' VALUES (' + vip_id + ',' + election_id
					relat_insert_str += ',' + str(NORMALIZED_LIST[element_name] + int(element.get('id')))
					relat_insert_str += ',' + str(NORMALIZED_LIST[elem.tag[:elem.tag.find("_id")]] + int(element.text))
					relat_insert_str += ')'
					cursor.execute(relat_insert_str)
				else:
					insert_str += ', ' + elem.tag
					if elem.text is None:
						val_str += ', \"\"'
					else:
						val_str += ', \"' + elem.text.replace('"', "'") + '\"'
		insert_str += val_str + ')'
		print insert_str
		cursor.execute(insert_str)

connection.commit()
