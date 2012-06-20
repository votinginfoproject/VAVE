import os
from hashlib import md5
import filetype as ft
import directorysearch as ds
import unpack
import formatcheck as fc
import feedtoflatfiles as ftff
from schemaprops import SchemaProps
from ConfigParser import ConfigParser
import psycopg2
import csv
from psycopg2 import extras
from datetime import date
from shutil import copyfile
import hashlib
from datetime import datetime

DIRECTORIES = {"temp":"temp/", "feeds":"feed_data/", "archives":"archives/", "reports":"reports/"}
SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"
CONFIG_FILE = "vip.cfg"
unpack_file = "test.tar.gz"
DEFAULT_ELECTION_ID = 1000
REQUIRED_FILES = ["source.txt", "election.txt"]
process_time = str(datetime.now())
file_time_stamp = process_time[:process_time.rfind(".")].replace(":","-").replace(" ","_")

def main():
	
	setup_directories()

	ftype = ft.get_type(unpack_file)

	unpack.unpack(unpack_file, DIRECTORIES["temp"])
	unpack.flatten_folder(DIRECTORIES["temp"])

	sp = SchemaProps()
	invalid_sections = []
	invalid_files = []
	valid_files = []

	if ds.file_by_name(CONFIG_FILE, DIRECTORIES["temp"]):
		invalid_sections = process_config(DIRECTORIES["temp"], DIRECTORIES["temp"] + CONFIG_FILE, sp)
	if ds.files_by_extension(".txt", DIRECTORIES["temp"]) > 0:
		invalid_files, valid_files = process_flatfiles(DIRECTORIES["temp"], sp)
	xml_files = ds.files_by_extension(".xml", DIRECTORIES["temp"])
	if len(xml_files) >= 1:
		ftff.feed_to_db_files(DIRECTORIES["temp"], DIRECTORIES["temp"] + xml_files[0], sp.full_header_data("db"), sp.version)
		os.remove(DIRECTORIES["temp"] + xml_files[0])

	feed_details = id_vals(DIRECTORIES["temp"])

	if "vip_id" not in feed_details:
		report_missing_source(feed_details, valid_files, invalid_files, invalid_sections)
		return
	elif "election_id" not in feed_details:
		report_missing_election(feed_details, valid_files, invalid_files, invalid_sections)
		return 
	elif len(xml_file) > 1:
		report_multiple_xml(feed_details, valid_files, invalid_files, invalid_sections)
		return

	process_files(DIRECTORIES["temp"], DIRECTORIES["archives"], vip_id, election_id)

def setup_directories():
	for directory in DIRECTORIES:
		if not os.path.exists(DIRECTORIES[directory]):
			os.mkdir(DIRECTORIES[directory])
		if directory == "temp":
			clear_directory(DIRECTORIES[directory])

def clear_directory(directory):
	for root, dirs, files in os.walk(directory):
		for f in files:
			os.unlink(os.path.join(root, f))
		for d in dirs:
			rmtree(os.path.join(root, d))

def report_missing_source(feed_details, valid_files, invalid_files, invalid_sections):
	if not os.path.exists(DIRECTORIES["reports"] + "unknown"):
		os.mkdir(DIRECTORIES["reports"] + "unknown")
	with open(DIRECTORIES["reports"] + "unknown/report_summary_" + file_time_stamp + ".txt", "w") as w:
		w.write("File Processed: " + unpack_file + "\n")
		w.write("Time Processed: " + process_time + "\n\n")
		w.write("----------------------\nFile Report\n----------------------\n\n")
		if len(invalid_sections) > 0:
			w.write("Invalid Sections: " + str(invalid_sections) + "\n")
		if len(invalid_files) > 0:
			w.write("Invalid Files: " + str(invalid_files) + "\n")
		if len(valid_files) > 0:
			w.write("Valid Files: " + str(valid_files) + "\n")
		w.write("Missing source information, could not process feed")

def report_missing_election(feed_details, valid_files, invalid_files, invalid_sections):
	directory = DIRECTORIES["reports"] + str(feed_details["vip_id"])
	fname = "report_summary_" + str(feed_details["vip_id"]) + "_" + file_time_stamp + ".txt"
	if not os.path.exists(directory):
		os.mkdir(directory)
		os.mkdir(directory + "/archives")
		os.mkdir(directory + "/current")
	else:
		clear_directory(directory + "/current")
	with open(directory + "/current/" + fname, "w") as w:
		w.write("File Processed: " + unpack_file + "\n")
		w.write("Time Processed: " + process_time + "\n\n")
		w.write("----------------------\nSource Data\n----------------------\n\n")
		w.write("Name: " + feed_details["name"] + "\n")
		w.write("Vip ID: " + str(feed_details["vip_id"]) + "\n")
		w.write("Datetime: " + str(feed_details["datetime"]) + "\n\n")
		w.write("----------------------\nFile Report\n----------------------\n\n")
		if len(invalid_sections) > 0:
			w.write("Invalid Sections: " + str(invalid_sections) + "\n")
		if len(invalid_files) > 0:
			w.write("Invalid Files: " + str(invalid_files) + "\n")
		if len(valid_files) > 0:
			w.write("Valid Files: " + str(valid_files) + "\n")
		w.write("Missing election information, could not process feed")
	copyfile(directory + "/current/" + fname, directory + "/archives" + fname)	

def report_multiple_xml(feed_details, valid_files, invalid_files, invalid_sections):
	directory = DIRECTORIES["reports"] + str(feed_details["vip_id"])
	fname = "report_summary_" + str(feed_details["vip_id"]) + "_" + feed_details["election_id"] + "_" + file_time_stamp + ".txt"
	if not os.path.exists(directory):
		os.mkdir(directory)
		os.mkdir(directory + "/archives")
		os.mkdir(directory + "/current")
	else:
		clear_directory(directory + "/current")
	with open(directory + "/current/" + fname, "w") as w:
		w.write("File Processed: " + unpack_file + "\n")
		w.write("Time Processed: " + process_time + "\n\n")
		w.write("----------------------\nSource Data\n----------------------\n\n")
		w.write("Name: " + feed_details["name"] + "\n")
		w.write("Vip ID: " + str(feed_details["vip_id"]) + "\n")
		w.write("Datetime: " + str(feed_details["datetime"]) + "\n\n")
		w.write("----------------------\nElection Data\n----------------------\n\n")
		w.write("Election ID: " + feed_details["election_id"] + "\n")
		w.write("Election Date: " + str(feed_details["election_date"]) + "\n")
		w.write("Election Type: " + str(feed_details["election_type"]) + "\n\n")
		w.write("----------------------\nFile Report\n----------------------\n\n")
		if len(invalid_sections) > 0:
			w.write("Invalid Sections: " + str(invalid_sections) + "\n")
		if len(invalid_files) > 0:
			w.write("Invalid Files: " + str(invalid_files) + "\n")
		if len(valid_files) > 0:
			w.write("Valid Files: " + str(valid_files) + "\n")
		w.write("Multiple xml files provided, could not process feed")
	copyfile(directory + "/current/" + fname, directory + "/archives" + fname)
	print feed_details
		
def process_files(feed_dir, archive_dir, vip_id, election_id):
#	conn = psycopg2.connect(host="localhost", database="vip_metadata", user="username", password="password")
	conn = psycopg2.connect(host="localhost", database="vip_metadata", user="jensen", password="gamet1me")
	cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

	file_list = os.listdir(self.directory)
	new_files = []
	for f in file_list:
		cursor.execute("SELECT hash FROM file_data WHERE file_name = '" + f + "' AND vip_id = " + str(vip_id) + " AND election_id = " + str(election_id))
		hash_val = cursor.fetchone()
		new_hash = file_hash(feed_dir + f)
		if not hash_val:
			cursor.execute("INSERT INTO file_data (vip_id, election_id, file_name, hash) VALUES (" + str(vip_id) + "," + str(election_id) + ",'" + f + "','" + new_hash + "')")
			new_files.append(f)
		if new_hash != hash_val:
			cursor.execute("UPDATE file_data SET hash = " + new_hash + " WHERE vip_id = " + str(vip_id) + " and election_id = " + str(election_id))
			new_files.append(f)

	archive_files(feed_dir, archive_dir, new_files)
	
	convert_files(directory, new_files, vip_id, election_id)

	update_db(directory, new_files)

def convert_files(directory, new_files, vip_id, election_id):
	
	for f in new_files:
		filename, extension = f.split(".")

def archive_files(feed_dir, archive_dir, file_list):
	cur_date = date.today().isoformat()
	for f in file_list:
		element_name, extension = f.lower().split(".")
		copyfile(feed_dir + f, archive_dir + element_name + "_" + str(cur_date) + ".txt")

def file_hash(fname):
	with open(fname, "rb") as fh:
		m = hashlib.md5()
		for data in fh.read(8192):
			m.update(data)
	return m.hexdigest()

def update_db(directory, files):
#	vip_conn = psycopg2.connect(host="localhost", database="vip", user="username", password="password")
	vip_conn = psycopg2.connect(host="localhost", database="vip", user="jensen", password="gamet1me")
	vip_cursor = vip_conn.cursor()
	SQL_STATEMENT = "COPY {0}({1}) FROM '{2}' WITH CSV HEADER"

	for f in files:
		r = csv.DictReader(open(directory+f, "r"))
		copy_statement = SQL_STATEMENT.format(f.split(".")[0].lower(), ",".join(r.fieldnames), "/"+f)
		print copy_statement
		vip_cursor.copy_expert(copy_statement, sys.stdin)
		vip_conn.commit()

def get_feed_details(directory):

	feed_details = {}
#	conn = psycopg2.connect(host="localhost", database="vip_metadata", user="username", password="password")
	conn = psycopg2.connect(host="localhost", database="vip_metadata", user="jensen", password="gamet1me")
	cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
	
	try:
		with open(directory + "source.txt", "r") as f:
			fdata = csv.DictReader(f)
			for row in fdata:
				feed_details["vip_id"] = row["vip_id"]
				feed_details["name"] = row["name"]
				feed_details["datetime"] = row["datetime"]
		with open(directory + "election.txt", "r") as f:
			fdata = csv.DictReader(f)
			for row in fdata:
				feed_details["election_date"] = row["date"]
				feed_details["election_type"] = row["election_type"]
	except:
		return feed_details
	
	cursor.execute("SELECT * FROM elections WHERE vip_id = " + str(feed_details["vip_id"]) + " AND election_date = '" + feed_details["election_date"] + "' AND election_type = '" + feed_details["election_type"] + "'")
	election_data = cursor.fetchone()
	if not election_data:
		cursor.execute("SELECT GREATEST(election_id) FROM elections")
		last_id = cursor.fetchone()
		if not last_id:
			new_id = DEFAULT_ELECTION_ID
		else:
			new_id = int(last_id) + 1
		cursor.execute("INSERT INTO elections (vip_id, election_date, election_type, election_id) VALUES (" + str(feed_details["vip_id"]) + "," + feed_details["election_date"] + "," + feed_details["election_type"] + "," + str(new_id) + ")")
		conn.commit()
		feed_details["election_id"] = new_id
		return feed_details
	else:
		feed_details["election_id"] = election_data["election_id"]
		return feed_details


#add in header to all valid formatted files, delete invalid files
def process_config(directory, config_file, schema_props):
	
	config = ConfigParser()
	config.read(config_file)
	sections = config.sections()
	if any(s not in schema_props.key_list("element") for s in sections):
		if all(s in schema_props.key_list("db") for s in sections):
			invalid_sections = fc.invalid_config_sections(directory, config_file, schema_props.full_header_data("db"))
		else:
			print "sections error!!!"
	else:
		invalid_sections = fc.invalid_config_sections(directory, config_file, schema_props.full_header_data("element"))

	for s in sections:
		fname = config.get(s, "file_name")
		header = config.get(s, "header")
		if s in invalid_sections:
			if os.path.exists(directory + fname):
				os.remove(directory + fname)
		else:
			with open(directory + s + "_temp.txt", "w") as w:
				w.write(header + "\n")
				with open(directory + fname, "r") as r:
					for line in r:
						w.write(line)
				os.remove(directory + fname)
			os.rename(directory + s + "_temp.txt", directory + fname)
	os.remove(config_file)
	return invalid_sections

#check flat file format, if element format, check then convert to element format
#if db format, check and then leave alone
def process_flatfiles(directory, schema_props):

	file_list = {}
	for f in os.listdir(directory):
		element_name, extension = f.lower().split(".")
		if extension == "txt" or extension == "csv":
			file_list[f] = element_name

	if any(vals not in schema_props.key_list("element") for vals in file_list.values()):
		if all(vals in schema_props.key_list("db") for vals in file_list.values()):
			invalid_files = fc.invalid_files(directory, file_list, schema_props.full_header_data("db"))
		else:
			print "file error!!!"
	else:
		invalid_files = fc.invalid_files(directory, file_list, schema_props.full_header_data("element"))
		for k, v in file_list.iteritems():
			if k in invalid_files:
				os.remove(directory + k)
			else:
				valid_files = convert_data(directory, k, v, schema_props.conversion_by_element(v))
	return invalid_files

#converts data from element format to db format. Currently opens and reads
#through the whole file each time, splitting on each row was actually slower
def convert_data(directory, fname, element, conversion_dict):
	files_used = []
	for conversion in conversion_dict:
		if conversion == element:
			continue
		with open(directory + fname, "r") as f:
			fdata = csv.DictReader(f)
			header = fdata.fieldnames
			output_list = []
			header_list = []
			for h in header:
				if h in conversion_dict[conversion]:
					header_list.append(conversion_dict[conversion][h])
					output_list.append(h)
			if len(output_list) > 1:
				files_used.append(conversion + ".txt")
				print "processing " + conversion
				with open(directory + conversion + ".txt", "w") as w:
					w.write(",".join(header_list) + "\n")
					for row in fdata:
						row_data = []
						for o in output_list:
							row_data.append(row[o])
						w.write(",".join(row_data) + "\n")
	element_conversion = conversion_dict[element]
	print "processing " + element
	files_used.append(element + ".txt")	
	with open(directory + fname, "r") as f:
		fdata = csv.DictReader(f)
		header = fdata.fieldnames
		output_list = []
		header_list = []
		for h in header:
			if h in element_conversion:
				header_list.append(element_conversion[h])
				output_list.append(h)
		with open(directory + element + "_temp.txt", "w") as w:
			w.write(",".join(header_list) + "\n")
			for row in fdata:
				row_data = []
				for o in output_list:
					row_data.append(row[o])
				w.write(",".join(row_data) + "\n")		
	os.remove(directory + fname)
	os.rename(directory + element + "_temp.txt", directory + fname)

if __name__ == "__main__":
	main()
