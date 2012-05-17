from schema import Schema
from urllib import urlopen
import MySQLdb as mdb
import sqlite3
import os
import csv

SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"

FEED_DIR = "../flat_files/"

schema = Schema(urlopen(SCHEMA_URL))

def get_fields(schema, elem_list):
	
	field_list = {}
	
	for elem_name in elem_list:

		subschema = schema.get_sub_schema(elem_name)
		e_list = {}

		if "elements" in subschema:
			for e in subschema["elements"]:
				e_name = e["name"]
				if e["type"] == "simpleAddressType":
					for s_e in simple_elements:
						e_list[e_name + "_" + s_e] = "simpleAddressType_" + e_name
				elif e["type"] == "detailAddressType":
					for d_e in detail_elements:
						e_list[e_name + "_" + d_e] = "detailAddressType_" + e_name
				elif "maxOccurs" in e and e["maxOccurs"] == "unbounded":
					e_list[e_name] = elem_name + "_" + e_name[:e_name.find("_id")]
				else:
					e_list[e_name] = elem_name
				if "simpleContent" in e:
					e_list[e_name] = elem_name + "_" + e_name[:e_name.find("_id")]
					for sc_attr in e["simpleContent"]["attributes"]:
						e_list[e_name + "_" + sc_attr["name"]] = elem_name + "_" + e_name[:e_name.find("_id")]
		if "attributes" in subschema:
			for a in subschema["attributes"]:
				e_list[a["name"]] = elem_name			
		field_list[elem_name] = {}
		field_list[elem_name] = e_list

	return field_list

def insert_values(table_name, keys_list, values_list):
	insert_statement = "INSERT IGNORE INTO " + table_name + " ("
	insert_statement += ",".join(keys_list) + ") "
	insert_statement += " VALUES ('" + "','".join(values_list) + "')"
#	print insert_statement
	cursor.execute(insert_statement)
	connection.commit()
	return cursor.lastrowid	
	
def insert_address(row_data, a_name, address_info):
	keys_list = []
	values_list = []
	for a in address_info["elements"]:
		values_list.append(row_data[a].replace("'", "").replace("/","-"))
		keys_list.append(a[len(a_name)+1:])
	return str(insert_values(address_info["type"], keys_list, values_list))

def insert_other(row_data, elem_name, table_name, o_elements):
	keys_list = ["vip_id", "election_id", elem_name+"_id"]
	values_list = [str(vip_id), str(election_id), row_data["id"]]
	for o in o_elements["elements"]:
		values_list.append(row_data[o])
		if o.startswith(o_elements["elements"][0]) and len(o) > len(o_elements["elements"][0]):
			o = o[len(o_elements["elements"][0])+1:]
		keys_list.append(o)
	insert_values(table_name, keys_list, values_list)

def insert_base_elements(row_data, table_name, base_elements):
	keys_list = ["vip_id", "election_id", "feed_id"]
	keys_list.extend(base_elements)
	values_list = [str(vip_id), str(election_id), row_data["id"]]
	for b in base_elements:
		values_list.append(row_data[b].replace("'", "").replace("/","-"))
	insert_values(table_name, keys_list, values_list)

simple_elements = schema.get_element_list("complexType", "simpleAddressType")
detail_elements = schema.get_element_list("complexType", "detailAddressType")
element_list = schema.get_element_list("element","vip_object")
elem_fields = get_fields(schema, element_list)
vip_id = 32
election_id = 1000
#connection = sqlite3.connect("localhost")
connection = mdb.connect("localhost", "vip", "username", "password")
cursor = connection.cursor()

for f in os.listdir(FEED_DIR):
	ename = f.split(".")[0].lower()

	if ename != "precinct_split": 
		continue
	
	if ename in element_list:

		print "inserting: " + ename
	
		data = csv.DictReader(open(FEED_DIR + f))
		header = data.fieldnames
		ff_list = elem_fields[ename]
		addresses = {}
		others = {}
		base_elements = []
	
		for h in header:
			if ff_list[h].startswith("simpleAddressType"):
				a_name = ff_list[h][len("simpleAddressType")+1:]
				if a_name not in addresses:
					addresses[a_name] = {"type":"simpleAddressType", "elements":[]}
					base_elements.append(a_name+"_id")
				addresses[a_name]["elements"].append(h)
			elif ff_list[h].startswith("detailAddressType"):
				a_name = ff_list[h][len("detailAddressType")+1:]
				if a_name not in addresses:
					addresses[a_name] = {"type":"detailAddressType", "elements":[]}
					base_elements.append(a_name+"_id")
				addresses[a_name]["elements"].append(h)
			elif ff_list[h] != ename:
				o_name = ff_list[h]
				if o_name not in others:
					others[o_name] = {"elements":[]}
				others[o_name]["elements"].append(h)
			elif h != "id":
				base_elements.append(h)
			

		for row in data:

			for a_name in addresses:
				row[a_name+"_id"] = insert_address(row, a_name, addresses[a_name])
			for o_name in others:
				insert_other(row, ename, o_name, others[o_name])
			insert_base_elements(row, ename, base_elements)

