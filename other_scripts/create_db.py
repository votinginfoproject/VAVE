from argparse import ArgumentParser
from urllib import urlopen
from schema import Schema
import sqlite3
import MySQLdb as mdb
import psycopg2

#for string formatting, "xs:" changes to "xml_" because ":" within format
#interprets the format statement as a range

TYPE_CONVERSIONS = {	"sqlite":	{"id":"INTEGER PRIMARY KEY AUTOINCREMENT", "xml_string":"TEXT", 
					"xml_integer":"INTEGER", "xml_dateTime":"TEXT", 
					"timestamp": "TEXT", "xml_date":"TEXT",
					"int": "INTEGER", "boolean": "INTEGER",
					"date_created": "DEFAULT CURRENT_TIMESTAMP NOT NULL",
					"date_modified": "DEFAULT CURRENT_TIMESTAMP NOT NULL"},
			"mysql":	{"id":"BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY", "xml_string":"VARCHAR(256)", 
					"xml_integer":"BIGINT", "xml_dateTime":"DATETIME", 
					"timestamp": "TIMESTAMP", "xml_date":"DATE",
					"int": "INTEGER", "boolean": "TINYINT(1)",
					"date_created": "DEFAULT now() ON UPDATE now()",
					"date_modified": "DEFAULT '0000-00-00 00:00:00'"}, 
			"postgres":	{"id":"SERIAL PRIMARY KEY", "xml_string":"VARCHAR(256)", 
					"xml_integer":"BIGINT", "xml_dateTime":"TIMESTAMP", 
					"timestamp": "TIMESTAMP", "xml_date":"DATE",
					"int": "INTEGER", "boolean": "BOOLEAN",
					"date_created": "DEFAULT CURRENT_TIMESTAMP",
					"date_modified": "DEFAULT CURRENT_TIMESTAMP"}} 

SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"

#default settings
db_type = "sqlite"
host = "localhost"
db_name = "vip"
username = "username"
password = "password"

def get_parsed_args():
	parser = ArgumentParser(description='create database from schema')

	parser.add_argument('-d', action='store', dest='db_type',
			help='database type, valid types are: sqlite, mysql, postgres')

	parser.add_argument('-u', action='store', dest='username',
			help='username to access the database')
	
	parser.add_argument('-p', action='store', dest='password',
			help='password for the database user')

	parser.add_argument('-n', action='store', dest='db_name',
			help='database name the data is stored in')

	parser.add_argument('-host', action='store', dest='host',
			help='database host address, database file location using sqlite')

	return parser.parse_args()

def timestamp_fields():
	return ", last_modified {timestamp} {date_modified}, date_created {timestamp} {date_created}"

def create_enum(simple, simple_elements):
	simple_elements = list(set(e.lower() for e in simple_elements)) #eliminate case from enums
	create_statement = "CREATE TYPE " + str(simple) 
	create_statement += " AS ENUM('" + "','".join(simple_elements) + "');"
	cursor.execute(create_statement)
	connection.commit()

def create_relational_table(name, element):

	ename1 = name
	ename2 = element["name"][:element["name"].find("_id")]

	create_statement = "CREATE TABLE " + ename1 + "_" + ename2
	create_statement += " ( id {id}, vip_id {int}, election_id {int}"
	create_statement += "," + ename1 + "_id {xml_integer}, "
	create_statement += ename2 + "_id {xml_integer}"

	if "simpleContent" in element and "attributes" in element["simpleContent"]:
		for attr in element["simpleContent"]["attributes"]:
			create_statement += ", " + attr["name"] + " {" + attr["type"] + "}"

	create_statement += timestamp_fields()
	create_statement += ", UNIQUE(vip_id, election_id, " + ename1 + "_id, " + ename2 + "_id))"
	create_statement = create_statement.replace("xs:", "xml_")
	create_statement = create_statement.format(**TYPE_CONVERSIONS[db_type])
	cursor.execute(create_statement)
	connection.commit()
	
def create_table(name, elements): 
	create_statement = "CREATE TABLE " + str(name) + " (id {id}"
	
	if name not in complex_types:
		if name != "source":
			create_statement += ", vip_id {int}"
		if name != "contest":
			create_statement += ", election_id {int}"
		create_statement += ", feed_id {xml_integer}"
		create_statement += ", is_used {boolean}"

	for e in elements:
		if not "name" in e:
			if "elements" in e:
				create_relational_table(name, e["elements"][0])
		elif e["type"] == "complexType":
			if "simpleContent" in e:
				create_relational_table(name, e)
		elif e["type"].startswith("xs:"):
			if "maxOccurs" in e and e["maxOccurs"] == "unbounded":
				create_relational_table(name, e)
			else:
				create_statement += ", " + e["name"] + " {" + e["type"] + "}"
		else:
			if e["type"] in simple_types:
				create_statement += ", " + e["name"]
				if db_type == "sqlite":
					create_statement += " TEXT"
				elif db_type == "mysql":
					simple_elements = list(set(elem.lower() for elem in schema.get_element_list("simpleType", e["type"])))
					create_statement += " ENUM('"
					create_statement += "','".join(simple_elements)
					create_statement += "')"
				elif db_type == "postgres":
					create_statement += " " + e["type"]	
			elif e["type"] in complex_types:
				create_statement += ", " + e["name"] + "_id {xml_integer}" 

	create_statement += timestamp_fields()
	if name not in complex_types:
		create_statement += ", UNIQUE(vip_id, election_id, feed_id)"
	create_statement += ")"
	create_statement = create_statement.replace("xs:", "xml_")
	create_statement = create_statement.format(**TYPE_CONVERSIONS[db_type])

	cursor.execute(create_statement)		
	connection.commit()

def create_triggers():
	if db_type == "postgres":
		create_trigger = "CREATE OR REPLACE FUNCTION update_last_modified() RETURNS TRIGGER AS $$ BEGIN NEW.lastmodified = NOW(); RETURN NEW; END; $$ LANGUAGE 'plpgsql'";
		cursor.execute(create_trigger)
		connection.commit()
	elif db_type == "sqlite":
		trigger_count = 0
		cursor.execute("SELECT * FROM sqlite_master WHERE type='table'")

		table_list = []
		for c in cursor:
			table_list.append(c[1])
		for table in table_list:
			if table.startswith(db_type):
				continue
			create_trigger = "CREATE TRIGGER update_last_modified{0} AFTER INSERT ON {1} BEGIN UPDATE {1} SET last_modified = datetime('now') WHERE id = new.{1}; END;".format(trigger_count, table)
			cursor.execute(create_trigger)
			connection.commit()
			trigger_count += 1;

parameters = get_parsed_args()

if parameters.db_type:
	db_type = parameters.db_type
if parameters.host:
	host = parameters.host
if parameters.db_name:
	db_name = parameters.db_name
if parameters.username:
	username = parameters.username
if parameters.password:
	password = parameters.password

if db_type == "sqlite":
	connection = sqlite3.connect(host)
	connection.row_factory = sqlite3.Row
elif db_type == "mysql":
	connection = mdb.connect(host, username, password, db_name)
elif db_type == "postgres":
	connection = psycopg2.connect(host=host, database=db_name, user=username, password=password)

cursor = connection.cursor()

fschema = urlopen(SCHEMA_URL)
schema = Schema(fschema)

complex_types = schema.get_complexTypes()
simple_types = schema.get_simpleTypes()
elements = schema.get_element_list("element", "vip_object")

if db_type == "postgres":
	for simple in simple_types:
		create_enum(simple, schema.get_element_list("simpleType", simple))

for complex_t in complex_types:
	
	sub_schema = schema.get_sub_schema(complex_t)
	if "elements" in sub_schema:
		create_table(complex_t, sub_schema["elements"])

for element in elements:
	create_table(element, schema.get_sub_schema(element)["elements"])

if db_type == "postgres" or db_type == "sqlite":
	create_triggers()
