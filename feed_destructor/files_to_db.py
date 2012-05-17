from schema import Schema
from urllib import urlopen
import MySQLdb as mdb
import psycopg2
import sqlite3
import os
import csv

SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"

FEED_DIR = "../flat_files/"

schema = Schema(urlopen(SCHEMA_URL))

addresses = ["simpleAddressType", "detailAddressType"]

def get_fields(schema, elem_list):
	
	field_list = {}
	
	for root_elem in elem_list:

		subschema = schema.get_sub_schema(root_elem)
		e_list = {}

		if "elements" in subschema:
			for sub_elem in subschema["elements"]:
				sub_elem_name = sub_elem["name"]
				if sub_elem["type"] == "simpleAddressType":
					for s_e in simple_elements:
						e_list[sub_elem_name + "_" + s_e] = {"table_name":"simpleAddressType", "db_key":s_e, "e_name":sub_elem_name}
				elif sub_elem["type"] == "detailAddressType":
					for d_e in detail_elements:
						e_list[sub_elem_name + "_" + d_e] = {"table_name":"simpleAddressType", "db_key":s_e, "e_name":sub_elem_name}
				elif "maxOccurs" in sub_elem and sub_elem["maxOccurs"] == "unbounded":
					e_list[sub_elem_name] = {"table_name":root_elem + "_" + sub_elem_name[:sub_elem_name.find("_id")], "db_key":sub_elem_name}
				else:
					e_list[sub_elem_name] = {"table_name":root_elem, "db_key":sub_elem_name}
				if "simpleContent" in sub_elem:
					e_list[sub_elem_name] = {"table_name":root_elem + "_" + sub_elem_name[:sub_elem_name.find("_id")], "db_key":sub_elem_name}
					for sc_attr in sub_elem["simpleContent"]["attributes"]:
						e_list[sub_elem_name + "_" + sc_attr["name"]] = {"table_name":root_elem + "_" + sub_elem_name[:sub_elem_name.find("_id")], "db_key":sc_attr["name"]}
		if "attributes" in subschema:
			for a in subschema["attributes"]:
				if a["name"] == "id":
					e_list[a["name"]] = {"table_name":root_elem, "db_key":"feed_id"}
				else:
					e_list[a["name"]] = {"table_name":root_elem, "db_key":a["name"]}
		field_list[root_elem] = {}
		field_list[root_elem] = e_list

	return field_list

simple_elements = schema.get_element_list("complexType", "simpleAddressType")
detail_elements = schema.get_element_list("complexType", "detailAddressType")
element_list = schema.get_element_list("element","vip_object")
elem_fields = get_fields(schema, element_list)
vip_id = 23
election_id = 1000
connection = psycopg2.connect(host="localhost", database="vip", user="vip", password="password")
#connection = mdb.connect("localhost", "vip", "gamet1me", "vip")
cursor = connection.cursor()

def get_address_object(row_data, address_elements, table_name, e_name, id_val):
	address_dict = {"name":table_name, "id":id_val, "e_name":e_name, "elements":{}}
	for a_e in address_elements:
		address_dict["elements"][address_elements[a_e]] = str(row_data[a_e]).replace("'","").replace("/","-")
	return address_dict

def get_relational_object(row_data, relational_elements, table_name):
	relat_dict = {"name":table_name, "elements":{}}
	for r_e in relational_elements:
		relat_dict["elements"][relational_elements[r_e]] = str(row_data[r_e])
	return relat_dict

def get_element_object(row_data, base_elements):
	element_dict = {}
	for b_l in base_elements:
		element_dict[base_elements[b_l]] = str(row_data[b_l]).replace("'","").replace("/","-")
	return element_dict

def insert_row(table_name, data):
	insert_statement = "INSERT INTO " + table_name + " ("
	keys = ""
	vals = ""
	for d in data:
		if len(data[d]) <= 0:
			continue
		keys = d + ","
		vals = data[d] + "','"
	insert_statement += keys[:-1] + ") "
	insert_statement += " VALUES ('" + vals[:-2] + ")" 
	cursor.execute(insert_statement)
	connection.commit()
	return str(cursor.lastrowid)

for f in os.listdir(FEED_DIR):
	ename = f.split(".")[0].lower()

	if ename != "precinct_split":
		continue
	
	if ename in element_list:

		data = csv.DictReader(open(FEED_DIR + f))
		header = data.fieldnames
		ff_list = elem_fields[ename]

		address_list = {}
		base_list = {"vip_id":"vip_id","election_id":"election_id"}
		relational_list = {}
	
		for h in header:
			table_name = ff_list[h]["table_name"]
			if table_name == ename:
				base_list[h] = ff_list[h]["db_key"]
			elif table_name in addresses:
				add_elem_name = ff_list[h]["e_name"]
				if add_elem_name not in address_list:
					address_list[add_elem_name] = {"table_name":table_name, "elements":{}}
				address_list[add_elem_name]["elements"][h] = ff_list[h]["db_key"]
			else:
				if table_name not in relational_list:
					relational_list[table_name] = {"elements":{"id":ename+"_id", "vip_id":"vip_id", "election_id":"election_id"}}
				relational_list[table_name]["elements"][h] = ff_list[h]["db_key"]
		print ename
		
		address_data = []
		relational_data = {}
		element_data = {}

		for row in data:
			row["vip_id"] = vip_id
			row["election_id"] = election_id
			id_val = row["id"]
			
			for a in address_list:
				address_data.append(get_address_object(row, address_list[a]["elements"], address_list[a]["table_name"], a, id_val))
			for r in relational_list:
				relat_id = row[r[len(ename)+1:]+"_id"]
				full_id = id_val + "_" + relat_id
				if len(relat_id) > 0 and full_id not in relational_data:
					relational_data[full_id] = get_relational_object(row, relational_list[r]["elements"], r)
			if id_val not in element_data:
				element_data[id_val] = get_element_object(row, base_list)

		for i in range(len(address_data)):
			element_data[address_data[i]["id"]][address_data[i]["e_name"]+"_id"] = insert_row(address_data[i]["name"], address_data[i]["elements"])
		for r in relational_data:
			insert_row(relational_data[r]["name"], relational_data[r]["elements"])
		for e in element_data:
			insert_row(ename, element_data[e])

