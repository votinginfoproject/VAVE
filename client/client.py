import sqlite3
from simpleformatcheck import SimpleFormatCheck
from urllib import urlopen
from sys import exit
from ConfigParser import ConfigParser
from datetime import datetime
from hashlib import md5
import put
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
	
	w = open(config.get("app_settings", "log_file"), "ab")
	w.write("******************"+str(datetime.now())+"*********************\n\n")
	
	if status == "invalid":
		w.write("Could not process data, missing files and/or no xml file provided\n")
		w.write("Missing files: " + str(fc.missing_files()) + "\n\n")
	if status == "empty":
		w.write("No update sent due to lack of file changes\n\n")
	if status == "success":
		w.write("Files successfully sent: " + str(files_to_send) + "\n")
		w.write("Invalid files that failed to send: " + str(fc.get_invalid_files().keys()) + "\n\n")

def has_changed(fname):
	cursor.execute("SELECT hash FROM file_data WHERE file_name = '" + fname + "'")
	new_hash = file_hash(fname)
	old_vals = cursor.fetchone()
	if not old_vals: 
		cursor.execute("INSERT INTO file_data (file_name, hash) VALUES('" + fname + "','" + new_hash + "')")
		connection.commit()
		return True
	elif old_vals[0] != new_hash: 
		cursor.execute("UPDATE file_data SET hash = '" + new_hash + "' WHERE file_name = '" + fname + "'")
		connection.commit()
		return True
	return False

def file_hash(fname):
	m = md5()
	with open(fname, "rb") as fh:
		for data in fh.read(8192):
			m.update(data)
	return m.hexdigest()

def send_files(files_to_send):
	
	output_file = config.get("local_settings", "output_file")
	output_url = config.get("connection_settings", "output_url")
	f = zipfile.ZipFile(output_file, "w")
	for name in files_to_send:
		f.write(name, os.path.basename(name), zipfile.ZIP_DEFLATED)
	f.close()
	return
	f = open(output_file, 'rb')
	put.putfile(f, output_url + output_file)
	f.close()

def get_xml():
	for fname in os.listdir(file_directory):
		if fname.endswith(".xml"):
			return fname

def clean_directory(directory):
	if not directory.endswith("/"):
		return directory + "/"
	return directory

config = ConfigParser()
config.read(CONFIG_FILE)

schema_file = get_schema_file()

file_directory = config.get("local_settings", "file_directory")
file_directory = clean_directory(file_directory)
fc = SimpleFormatCheck(schema_file, file_directory)

fc.validate_files()
valid_files = fc.get_valid_files()

connection = sqlite3.connect(config.get("app_settings", "db_host"))
cursor = connection.cursor()
setup_db()

default_files = config.get("app_settings","default_files").split(",")

files_to_send = []

if config.has_option("local_settings","feed_data") and len(config.get("local_settings","feed_data")) > 0:
	feed_file = config.get("local_settings","feed_data")
	full_name = file_directory + feed_file
	if has_changed(full_name):
		files_to_send.append(full_name)
else:
	for fname in os.listdir(file_directory):
		full_name = file_directory + fname
		if fname.endswith(".xml") or fname == CONFIG_FILE:
			if has_changed(full_name):
				files_to_send.append(full_name)
		elif fname in valid_files:
			if has_changed(full_name):
				files_to_send.append(full_name)

if len(files_to_send) > 0:
	xml_doc = get_xml()
	if xml_doc and not any(f.endswith(".xml") for f in files_to_send):
		files_to_send.append(file_directory + xml_doc)
	elif not xml_doc:
		for f in default_files:
			if (file_directory + f not in files_to_send) and (f in os.listdir(file_directory)):
				files_to_send.append(file_directory + f)
			elif f not in os.listdir(file_directory):
				write_logs("invalid")
				exit(0)
	send_files(files_to_send)
	write_logs("success")
else :
	write_logs("empty")

