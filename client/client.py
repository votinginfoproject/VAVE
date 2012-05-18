import sqlite3
from simpleformatcheck import SimpleFormatCheck
from urllib import urlopen
from sys import exit
from ConfigParser import ConfigParser
from datetime import datetime
from hashlib import md5
import os
import zipfile
	
CONFIG_FILE = "client.cfg"

def get_schema_file():
	location_type = config.get("schema", "location_type")
	location = config.get("schema", "location")
	
	if location_type == "url":
		return urlopen(location)
	elif location_type == "file":
		return open(location)

def setup_db():
	cursor.execute("CREATE TABLE IF NOT EXISTS file_data (file_name TEXT, hash TEXT)")

def write_logs(status):
	
	w = open(config.get("app_settings", "log_file"), "a")
	w.write("******************"+str(datetime.utcnow().isoformat())+"*********************\n\n")
	
	if status == "invalid":
		w.write("Could not process data, missing files and/or no xml file provided\n")
		w.write("Missing files: " + str(fc.missing_files()) + "\n\n")
	if status == "empty":
		w.write("No update sent due to lack of file changes\n\n")
	if status == "success":
		w.write("Files successfully sent: " + str(files_to_send) + "\n")
		w.write("Invalid files that failed to send: " + str(fc.get_invalid_files()) + "\n\n")

def has_changed(fname):
	cursor.execute("SELECT hash FROM file_data WHERE file_name = '" + fname + "'")
	new_hash = file_hash(fname)
	old_vals = cursor.fetchone()
	if not old_vals: 
		cursor.execute("INSERT INTO file_data (file_name, hash) VALUES('" + fname + "','" + new_hash + "')")
		connection.commit()
		return True
	elif old_vals[0] != new_hash or fname.find("source") >= 0 or fname.find("election") >= 0: #election and source are always sent, unless and xml file is provided and the elements are contained there
		cursor.execute("UPDATE file_data SET hash = '" + new_hash + "' WHERE file_name = '" + fname + "'")
		connection.commit()
		return True
	return False

def file_hash(fname):
	with open(fname, "rb") as fh:
		m = md5()
		for data in fh.read(8192):
			m.update(data)
	return m.hexdigest()

def send_files(files_to_send):
	f = zipfile.ZipFile(config.get("local_settings","output_file"), "w")
	for name in files_to_send:
		f.write(name, os.path.basename(name), zipfile.ZIP_DEFLATED)
	f.close()

config = ConfigParser()
config.read(CONFIG_FILE)

schema_file = get_schema_file()

file_directory = config.get("local_settings", "file_directory")
fc = SimpleFormatCheck(schema_file, file_directory)

if not fc.validate_files():
	write_logs("invalid")
	exit(0)

valid_files = fc.get_valid_files()

connection = sqlite3.connect(config.get("app_settings", "db_host"))
cursor = connection.cursor()
setup_db()

files_to_send = []

for fname in os.listdir(file_directory):
	full_name = file_directory + "/" + fname
	if fname.endswith(".xml") or fname == CONFIG_FILE:
		if has_changed(full_name):
			files_to_send.append(full_name)
	elif fname in valid_files:
		if has_changed(full_name):
			files_to_send.append(full_name)

if len(files_to_send) > 0:
	send_files(files_to_send)
	write_logs("success")
else:
	write_logs("empty")

