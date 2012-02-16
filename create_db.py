import argparser
import urllib
import schema
import sqlite3
import MySQLdb as mdb
import psycopg2

TYPES_CONVERSIONS = {	"sqlite3":	{"id":"TEXT", "xs:string":"TEXT", 
					"xs:integer":"INTEGER", "xs:dateTime":"TEXT", 
					"xs:date":"TEXT"},
			"mysql":	{"id":"VARCHAR(16)", "xs:string":"VARCHAR(256)", 
					"xs:integer":"BIGINT", "xs:dateTime":"DATETIME", 
					"xs:date":"DATE"}, 
			"postgres":	{"id":"VARCHAR(16)", "xs:string":"VARCHAR(256)", 
					"xs:integer":"BIGINT", "xs:dateTime":"TIMESTAMP", 
					"xs:date":"DATE"}}

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
	create_statement = "CREATE TYPE " + str(element) + " AS ENUM('"
	create_statement += "','".join(simple_elements)
	create_statement += "');"
	cursor.execute(create_statement)
	connection.commit()

def get_id_type(db_type):
	if database_type == "sqlite3":
		return "INTEGER PRIMARY KEY AUTOINCREMENT"
	elif database_type == "mysql":
		return "BIGINT PRIMARY KEY AUTO_INCREMENT"
	elif database_type == "postgres":
		return "SERIAL PRIMARY KEY"
	
def complex_create(data):
	for element in data:
		create_statement = "CREATE TABLE " + str(element) + " (id " 
		#create_statement += get_id_type(db_type)
		for e in data[element]["elements"]:
			if e["name"] == "None":
				continue
			elif e["type"].startswith("xs:"):
				create_statement += ", " + str(e["name"]) + " " + TYPES[database_type][e["type"]]
			else:
				if e["type"] in simple_types:
					create_statement += ", " + str(e["name"])
					if database_type == "sqlite3":
						create_statement += " TEXT"
					elif database_type == "mysql":
						create_statement += " ENUM("
						for e_type in simple_types[e["type"]]["elements"]:
							create_statement += e_type + ","
						create_statement = create_statement[:-1] + ")" #remove trailing ',' from ENUM
				elif e["type"] in complex_types:
					create_statement += ", " + str(e["name"]) + "_id " + TYPES[database_type]["xs:integer"]
		create_statement += ");"
		cursor.execute(create_statement)		
		if database_type == "postgres":
			connection.commit()
	return create_statement

#default settings: 
db_type = "sqlite3"
db_name = "vip"
host = "localhost"
username = "username"
password = "password"

#extra columns added in: auto-incremented bigint id, received_id, vip_id, election_id, last_updated, date_created, normalized_id, is_used
#exceptions for state, source, and election (shorter id's etc)

#cursor needs to be a script level variable since other mysqldb does not have 'execute script' we will have to execute every table create statement individually
#Foreign keys are not enforced
#postgresql does not have tinyint to treat as a boolean, only smallint

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

#based on the database_type, create the correct connection type, use the same cursor for all stuff since all types use the same object

def element_create(data):
	for element in data:
		create_statement = "CREATE TABLE " + str(element["name"]) + " (id " + TYPES[database_type]["id"] + " PRIMARY KEY, element_id " + TYPES[database_type]["xs:integer"]
		for e in element["elements"]:
			if e["name"] == "None":
				continue
			elif e["type"].startswith("xs:"):
				create_statement += ", " + str(e["name"]) + " " + TYPES[database_type][e["type"]]
			else:
				if e["type"] in simple_types:
					create_statement += ", " + str(e["name"])
					if database_type == "sqlite3":
						create_statement += " TEXT"
					elif database_type == "mysql":
						create_statement += " ENUM("
						for e_type in simple_types[e["type"]]["elements"]:
							create_statement += "'" + e_type + "',"
						create_statement = create_statement[:-1] + ")" #remove trailing ',' from ENUM
					elif database_type == "postgres":
						create_statement += " " + str(e["type"])
				elif e["type"] in complex_types:
					create_statement += ", " + str(e["name"]) + "_id " + TYPES[database_type]["xs:integer"]
					#could add the foeign key stuff for postgres and mysql
		create_statement += ");"		
		print create_statement
		cursor.execute(create_statement)		
		if database_type == "postgres":
			connection.commit()
	return create_statement



if database_type == "postgres":
	create_enums(simple_types)
element_create(data["elements"])
complex_create(complex_types)

connection.commit()
