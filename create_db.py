import argparse
import urllib
import schema
import sqlite3
import MySQLdb as mdb
import psycopg2

#TODO: Set up time stamps correctly

TYPE_CONVERSIONS = {	"sqlite3":	{"id":"INTEGER PRIMARY KEY", "xs:string":"TEXT", 
					"xs:integer":"INTEGER", "xs:dateTime":"TEXT", 
					"timestamp": "TEXT", "xs:date":"TEXT",
					"int": "INTEGER", "boolean": "INTEGER"},
			"mysql":	{"id":"BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY", "xs:string":"VARCHAR(256)", 
					"xs:integer":"BIGINT", "xs:dateTime":"DATETIME", 
					"timestamp": "TIMESTAMP", "xs:date":"DATE",
					"int": "INTEGER", "boolean": "TINYINT(1)"}, 
			"postgres":	{"id":"SERIAL PRIMARY KEY", "xs:string":"VARCHAR(256)", 
					"xs:integer":"BIGINT", "xs:dateTime":"TIMESTAMP", 
					"timestamp": "TIMESTAMP", "xs:date":"DATE",
					"int": "INTEGER", "boolean": "BOOLEAN"}} 

SCHEMA_URL = "http://election-info-standard.googlecode.com/files/vip_spec_v3.0.xsd"

def get_parsed_args():
	parser = argparse.ArgumentParser(description='create database from schema')

	parser.add_argument('-d', action='store', dest='db_type',
			help='database type, valid types are: sqlite3, mysql, postgres')

	parser.add_argument('-u', action='store', dest='username',
			help='username to access the database')
	
	parser.add_argument('-p', action='store', dest='password',
			help='password for the database user')

	parser.add_argument('-n', action='store', dest='db_name',
			help='database name the data is stored in')

	parser.add_argument('-host', action='store', dest='host',
			help='database host address, database file location using sqlite3')

	return parser.parse_args()

def create_enum(simple, simple_elements):
	simple_elements = list(set(e.lower() for e in simple_elements)) #eliminate case from enums
	create_statement = "CREATE TYPE " + str(simple) 
	create_statement += " AS ENUM('" 
	create_statement += "','".join(simple_elements)
	create_statement += "');"
	cursor.execute(create_statement)
	connection.commit()
	
def create_table(name, elements): #might be more efficient/pythonic to make a mapping, use names/type added in sync
	create_statement = "CREATE TABLE " + str(name) 
	create_statement += " (id " + TYPE_CONVERSIONS[db_type]["id"]
	
	if name not in complex_types:
		create_statement += ", normalized_id " + TYPE_CONVERSIONS[db_type]["xs:integer"]
		create_statement += ", received_id " + TYPE_CONVERSIONS[db_type]["xs:integer"]
		if name != "source":
			create_statement += ", vip_id " + TYPE_CONVERSIONS[db_type]["int"]
		if name != "contest":
			create_statement += ", election_id " + TYPE_CONVERSIONS[db_type]["int"]
		create_statement += ", is_used " + TYPE_CONVERSIONS[db_type]["boolean"]

	for e in elements:
		if e["name"] == "None":
			continue
		if e["type"] == "complexType":
			if "simpleContent" in e:
				create_relation_table = "CREATE TABLE " + str(name) + "_" + e["name"][:e["name"].find("_id")]
				create_relation_table += "(vip_id " + TYPE_CONVERSIONS[db_type]["xs:integer"]
				create_relation_table += ",election_id " + TYPE_CONVERSIONS[db_type]["xs:integer"]
				create_relation_table += "," + str(name) + "_id " + TYPE_CONVERSIONS[db_type]["xs:integer"]
				create_relation_table += "," + e["name"] + " " + TYPE_CONVERSIONS[db_type]["xs:integer"]
				for attr in e["attributes"]:
					create_relation_table += "," + attr["name"] + " " + TYPE_CONVERSIONS[db_type][attr["type"]]
				create_relation_table += ")"
				cursor.execute(create_relation_table)
				connection.commit()
		elif e["type"].startswith("xs:"):
			if "maxOccurs" in e and e["maxOccurs"] == "unbounded":
				create_relation_table = "CREATE TABLE " + str(name) + "_" + e["name"][:e["name"].find("_id")]
				create_relation_table += "(vip_id " + TYPE_CONVERSIONS[db_type]["xs:integer"]
				create_relation_table += ",election_id " + TYPE_CONVERSIONS[db_type]["xs:integer"]
				create_relation_table += "," + str(name) + "_id " + TYPE_CONVERSIONS[db_type]["xs:integer"]
				create_relation_table += "," + e["name"] + " " + TYPE_CONVERSIONS[db_type]["xs:integer"] + ")"
				cursor.execute(create_relation_table)
				connection.commit()
			else:
				create_statement += ", " + str(e["name"]) 
				create_statement += " " + TYPE_CONVERSIONS[db_type][e["type"]]
		else:
			if e["type"] in simple_types:
				create_statement += ", " + str(e["name"])
				if db_type == "sqlite3":
					create_statement += " TEXT"
				elif db_type == "mysql":
					simple_elements = list(set(elem.lower() for elem in schema.get_element_list("simpleType", e["type"])))
					create_statement += " ENUM('"
					create_statement += "','".join(simple_elements)
					create_statement += "')"
				elif db_type == "postgres":
					create_statement += " " + e["type"]	
			elif e["type"] in complex_types:
				create_statement += ", " + str(e["name"]) + "_id " 
				create_statement += TYPE_CONVERSIONS[db_type]["xs:integer"]

	create_statement += ", last_modified " + TYPE_CONVERSIONS[db_type]["timestamp"]
	if db_type == "sqlite3":
		create_statement += " DEFAULT CURRENT_TIMESTAMP NOT NULL"
	elif db_type == "mysql":
		create_statement += " default now() on update now() "
	elif db_type == "postgres":
		create_statement += " SET DEFAULT CURRENT_TIMESTAMP "
	create_statement += ", date_created " + TYPE_CONVERSIONS[db_type]["timestamp"]
	if db_type == "sqlite3":
		create_statement += " DEFAULT CURRENT_TIMESTAMP NOT NULL"
	if db_type == "mysql":
		create_statement += " default '0000-00-00 00:00:00' "
	elif db_type == "postgres":
		create_statement += " SET DEFAULT CURRENT_TIMESTAMP "
	create_statement += ");"

	cursor.execute(create_statement)		
	connection.commit()
	if db_type == "postgres":
		create_trigger = "CREATE OR REPLACE FUNCTION update_last_modified() RETURNS TRIGGER AS $$ BEGIN NEW.lastmodified = NOW(); RETURN NEW; END; $$ LANGUAGE 'plpqsql'";
		cursor.execute(create_trigger)
		connection.commit()
	elif db_type == "sqlite3":
		create_trigger = "CREATE TRIGGER update_last_modified" + str(trigger_count) + " AFTER INSERT ON " + str(name) + " BEGIN UPDATE " + str(name) + " SET last_modified = datetime('now') WHERE id = new." + str(name) + "; END;"
		cursor.execute(create_trigger)
		connection.commit()
		trigger_count += 1;

#default settings: 
db_type = "sqlite3"
db_name = "vip"
host = "localhost"
username = "username"
password = "password"
trigger_count = 0;

parameters = get_parsed_args()

if parameters.db_type:
	db_type = parameters.db_type
if parameters.db_name:
	db_name = parameters.db_name
if parameters.host:
	host = parameters.host
if parameters.username:
	username = parameters.username
if parameters.password:
	password = parameters.password

if db_type == "sqlite3":
	connection = sqlite3.connect(host)
elif db_type == "mysql":
	connection = mdb.connect(host, username, password, db_name)
elif db_type == "postgres":
	connection = psycopg2.connect(host=host, database=db_name, user=username, password=password)

cursor = connection.cursor()

fschema = urllib.urlopen(SCHEMA_URL)
schema = schema.schema(fschema)

complex_types = schema.get_complexTypes()
simple_types = schema.get_simpleTypes()
elements = schema.get_element_list("element", "vip_object")

if db_type == "postgres":
	for simple in simple_types:
		create_enum(simple, schema.get_element_list("simpleType", simple))

for complex_t in complex_types:
	create_table(complex_t, schema.get_sub_schema(complex_t)["elements"])

for element in elements:
	create_table(element, schema.get_sub_schema(element)["elements"])
