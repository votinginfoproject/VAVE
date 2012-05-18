from schema import Schema
from urllib import urlopen
import MySQLdb as mdb
import psycopg2
import sqlite3
import os
import csv
import sys

SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"

FEED_DIR = "../flat_files/"

schema = Schema(urlopen(SCHEMA_URL))

addresses = ["simpleAddressType", "detailAddressType"]

def get_fields(schema, elem_list):
	
	field_list = {}
	
	for root_elem in elem_list:
		
		e_list = {root_elem:{}}
		subschema = schema.get_sub_schema(root_elem)

		if "elements" in subschema:
			for sub_elem in subschema["elements"]:
				sub_elem_name = sub_elem["name"]
				if sub_elem["type"] == "simpleAddressType":
					for s_e in simple_elements:
						e_list[root_elem][sub_elem_name + "_" + s_e] = sub_elem_name + "_" + s_e
				elif sub_elem["type"] == "detailAddressType":
					for d_e in detail_elements:
						e_list[root_elem][sub_elem_name + "_" + d_e] = sub_elem_name + "_" + d_e
				elif "maxOccurs" in sub_elem and sub_elem["maxOccurs"] == "unbounded":
					t_name = root_elem + "_" + sub_elem_name[:sub_elem_name.find("_id")]
					if t_name not in e_list:
						e_list[t_name] = {"id":root_elem+"_id"}
					e_list[t_name][sub_elem_name] = sub_elem_name
				elif "simpleContent" in sub_elem:
					t_name = root_elem + "_" + sub_elem_name[:sub_elem_name.find("_id")]
					if t_name not in e_list:
						e_list[t_name] = {"id":root_elem+"_id"}
					e_list[t_name][sub_elem_name] = sub_elem_name
					for sc_attr in sub_elem["simpleContent"]["attributes"]:
						e_list[t_name][sub_elem_name + "_" + sc_attr["name"]] = sc_attr["name"]
				else:
					e_list[root_elem][sub_elem_name] = sub_elem_name
		if "attributes" in subschema:
			for a in subschema["attributes"]:
				if a["name"] == "id":
					e_list[root_elem]["feed_id"] = a["name"]
				else:
					e_list[root_elem][a["name"]] = a["name"]
		field_list[root_elem] = e_list

	return field_list

simple_elements = schema.get_element_list("complexType", "simpleAddressType")
detail_elements = schema.get_element_list("complexType", "detailAddressType")
element_list = schema.get_element_list("element","vip_object")
elem_fields = get_fields(schema, element_list)
vip_id = 23
election_id = 1000
connection = psycopg2.connect(host="localhost", database="vip", user="username", password="password")
cursor = connection.cursor()

for f in os.listdir(FEED_DIR):
	ename = f.split(".")[0].lower()

	if ename in element_list:

		data = csv.DictReader(open(FEED_DIR + f))
		header = data.fieldnames
		table_vals = elem_fields[ename]

		insert_format = {}
		
		for h in header:
			for t_v in table_vals:
				if h in table_vals[t_v]:
					if t_v not in insert_format:
						insert_format[t_v] = {"vip_id":"vip_id", "election_id":"election_id"}
					if t_v == ename:
						insert_format[t_v]["id"] = "feed_id"
					insert_format[t_v][h] = table_vals[t_v][h]
	
		output_data = {}
		for key in insert_format:
			output_data[key] = {}
		
		for row in data:
			row_id = row["id"]
			row["vip_id"] = vip_id
			row["election_id"] = election_id
			for table in insert_format:
				if table == ename:
					if row_id not in output_data[table]:
						output_data[table][row_id] = {} 
						for key in insert_format[table]:
							output_data[table][row_id][insert_format[table][key]] = row[key]
				else:
					relat_id = row[table[len(ename)+1:]+"_id"]
					full_id = row_id + "_" + relat_id
					if len(relat_id) > 0 and full_id not in output_data[table]:
						output_data[table][full_id] = {}
						for key in insert_format[table]:
							output_data[table][full_id][insert_format[table][key]] = row[key]		
		for key in output_data:
			if len(output_data[key]) <= 0:
				continue
			for row in output_data[key]:
				temp_id = row
				break
			w = csv.DictWriter(open(FEED_DIR + "database_files/" + key + ".txt", "w"), fieldnames=output_data[key][temp_id].keys())
			w.writeheader()
			for row in output_data[key]:
				w.writerow(output_data[key][row])

SQL_STATEMENT = "COPY {0}({1}) FROM '{2}' WITH CSV HEADER"

for f in os.listdir(FEED_DIR + "database_files"):
	r = csv.DictReader(open(FEED_DIR+"database_files/"+f, "r"))
	copy_statement = SQL_STATEMENT.format(f.split(".")[0], ",".join(r.fieldnames), f)
	cursor.copy_expert(copy_statement, sys.stdin)
	connection.commit()
