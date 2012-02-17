import argparser
import urllib
import schema
import sqlite3
import MySQLdb as mdb
import psycopg2

TYPES_CONVERSIONS = {	"sqlite3":	{"id":"TEXT", "xs:string":"TEXT", 
					"xs:integer":"INTEGER", "xs:dateTime":"TEXT", 
					"timestamp": "TEXT", "xs:date":"TEXT",
					"int": "INTEGER", "boolean": "INTEGER"},
			"mysql":	{"id":"VARCHAR(16)", "xs:string":"VARCHAR(256)", 
					"xs:integer":"BIGINT", "xs:dateTime":"DATETIME", 
					"timestamp": "TIMESTAMP", "xs:date":"DATE",
					"int": "INTEGER", "boolean": "TINYINT(1)"}, 
			"postgres":	{"id":"VARCHAR(16)", "xs:string":"VARCHAR(256)", 
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

def create_enums(simple, simple_elements):
	simple_elements = list(set(e.lower() for e in simple_elements)) #eliminate case from enums
	create_statement = "CREATE TYPE " + str(element) 
				+ " AS ENUM('" 
				+ "','".join(simple_elements)
				+ "');"
	cursor.execute(create_statement)
	connection.commit()
	
def create_tables(name, elements): #might be more efficient to make a mapping/pythonic, use names/type added in sync
	create_statement = "CREATE TABLE " + str(name) 
				+ " (id " + TYPE_CONVERSIONS[db_type]["id"]
	
	if name not in complex_types:
		create_statement += ", normalized_id " + TYPE_CONVERSIONS[db_type]["xs:integer"]
					+ ", received_id " + TYPE_CONVERSIONS[db_type]["xs:integer"]
					+ ", vip_id " + TYPE_CONVERSIONS[db_type]["int"]
					+ ", election_id " + TYPE_CONVERSIONS[db_type]["int"]
					+ ", is_used " + TYPE_CONVERSIONS[db_type]["boolean"]

	for e in elements:
		if e["name"] == "None":
			continue
		elif e["type"].startswith("xs:"):
			create_statement += ", " + str(e["name"]) 
						+ " " + TYPE_CONVERSIONS[db_type][e["type"]]
		else:
			if e["type"] in simple_types:
				create_statement += ", " + str(e["name"])
				if db_type == "sqlite3":
					create_statement += " TEXT"
				elif db_type == "mysql":
					create_statement += " ENUM('"
								+ "','".join(simple_types[e["type"]["elements"])
								+ "')"
				elif db_type == "postgres":
					create_statement += " " + e["type"]	
			elif e["type"] in complex_types:
				create_statement += ", " + str(e["name"]) + "_id " 
							+ TYPE_CONVERSIONS[db_type]["xs:integer"]

	create_statement += ", last_updated " + TYPES[database_type]["timestamp"]
				+ ", date_created " + TYPES[database_type]["timestamp"]
				+ ");"

	cursor.execute(create_statement)		
	connection.commit()

#default settings: 
db_type = "sqlite3"
db_name = "vip"
host = "localhost"
username = "username"
password = "password"

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

if database_type == "sqlite3":
	connection = sqlite3.connect(host)
elif database_type == "mysql":
	connection = mdb.connect(host, username, password, database_name)
elif database_type == "postgres":
	connection = psycopg2.connect(host=host, database=database_name, user=username, password=password)

cursor = connection.cursor()

fschema = urllib.urlopen(SCHEMA_URL)
schema = schema.schema(fschema)

complex_types = schema.get_complexTypes()
simple_types = schema.get_simpleTypes()

if database_type == "postgres":
	for simple in simple_types:
		create_enum(simple, schema.get_element_list("simpleType", simple))

#TODO: get elements/type to pass in, might need another schema function
for complex_t in complex_types:
	create_element(complex_t, schema.get_elements("complexType", complex_t))

#TODO: change to elements
for complex_t in complex_types:
	create_element(complex_t, schema.get_elements("complexType", complex_t))
