import sqlite3
import MySQLdb as mdb
import psycopg2
from argparse import ArgumentParser

TYPE_CONVERSIONS = {	"sqlite":	{"id":"INTEGER PRIMARY KEY AUTOINCREMENT", "xml_string":"TEXT", 
					"xml_integer":"INTEGER", "xml_dateTime":"TEXT", 
					"timestamp": "TEXT", "xml_date":"TEXT",
					"int": "INTEGER", "boolean": "INTEGER",
					"date_created": "DEFAULT CURRENT_TIMESTAMP NOT NULL",
					"date_modified": "DEFAULT CURRENT_TIMESTAMP NOT NULL"},
			"mysql":	{"id":"BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY", "xml_string":"VARCHAR(1024)", 
					"xml_integer":"BIGINT", "xml_dateTime":"DATETIME", 
					"timestamp": "TIMESTAMP", "xml_date":"DATE",
					"int": "INTEGER", "boolean": "TINYINT(1)",
					"date_created": "DEFAULT now() ON UPDATE now()",
					"date_modified": "DEFAULT '0000-00-00 00:00:00'"}, 
			"postgres":	{"id":"SERIAL PRIMARY KEY", "xml_string":"VARCHAR(1024)", 
					"xml_integer":"BIGINT", "xml_dateTime":"TIMESTAMP", 
					"timestamp": "TIMESTAMP", "xml_date":"DATE",
					"int": "INTEGER", "boolean": "BOOLEAN",
					"date_created": "DEFAULT CURRENT_TIMESTAMP",
					"date_modified": "DEFAULT CURRENT_TIMESTAMP"}} 

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

queries = ["CREATE TABLE elections (id {id}, vip_id {int}, election_date {timestamp}, election_type {xml_string}, election_id {int})", "CREATE TABLE file_data (vip_id {int}, election_id {int}, file_name {xml_string}, hash {xml_string})", "CREATE TABLE feed_data (vip_id {int}, election_id {int}, element {xml_string}, originial_count {int}, final_count {int})"]

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

for q in queries:
	create_statement = q.format(**TYPE_CONVERSIONS[db_type])
	cursor.execute(create_statement)
	connection.commit()
