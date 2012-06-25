import os
from hashlib import md5
import filetype as ft
import directorytools as dt
import unpack
import formatcheck as fc
import feedtoflatfiles as ftff
import errorreports as er
from schemaprops import SchemaProps
from ConfigParser import ConfigParser
import psycopg2
import csv
from psycopg2 import extras
from shutil import copyfile
import hashlib
from datetime import datetime
from time import strptime

DIRECTORIES = {"temp":"temp/", "archives":"archives/"}
SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"
CONFIG_FILE = "vip.cfg"
unpack_file = "test.zip"
DEFAULT_ELECTION_ID = 1000
REQUIRED_FILES = ["source.txt", "election.txt"]
process_time = str(datetime.now())
file_time_stamp = process_time[:process_time.rfind(".")].replace(":","-").replace(" ","_")

def main():

	print "setting up directories..."
	
	dt.clear_or_create("temp/")
	dt.create_directory("archives/")
	
	print "done setting up directories"

	ftype = ft.get_type(unpack_file)

	print "unpacking and flattening files..."

	unpack.unpack(unpack_file, DIRECTORIES["temp"])
	unpack.flatten_folder(DIRECTORIES["temp"])

	print "done unpacking and flattening"

	sp = SchemaProps()
	feed_details = {"file":unpack_file, "process_time":process_time, "file_time_stamp":file_time_stamp}
	invalid_sections = []
	invalid_files = []
	valid_files = []

	print "converting to db style flat files...."

	if dt.file_by_name(CONFIG_FILE, DIRECTORIES["temp"]):
		invalid_sections = process_config(DIRECTORIES["temp"], DIRECTORIES["temp"] + CONFIG_FILE, sp)
	if dt.files_by_extension(".txt", DIRECTORIES["temp"]) > 0:
		invalid_files, valid_files = process_flatfiles(DIRECTORIES["temp"], sp)
	print "processing xml files..."
	xml_files = dt.files_by_extension(".xml", DIRECTORIES["temp"])
	print xml_files
	if len(xml_files) >= 1:
		ftff.feed_to_db_files(DIRECTORIES["temp"], xml_files[0], sp.full_header_data("db"), sp.version)
		os.remove(xml_files[0])

	print "done processing xml files"

	print "getting feed details..."
	feed_details.update(get_feed_details(DIRECTORIES["temp"]))
	print "done getting feed details"

	print "writing initial report..."
	er.report_summary(feed_details, valid_files, invalid_files, invalid_sections)
	if "vip_id" not in feed_details or "election_id" not in feed_details:
		return
	print "done writing initial report"

	print "converting to full db files...."
	element_counts = convert_to_db_files(feed_details, DIRECTORIES["temp"], sp)
	print element_counts
	print "done converting to full db files"

#	update_data(feed_details, element_counts, DIRECTORIES["temp"], DIRECTORIES["archives"])	

	er.more_summary(feed_details, element_counts)

def update_data(feed_details, file_data, directory, archives):

	meta_conn = psycopg2.connect(host="localhost", database="vip_metadata", user="username", password="password")
	meta_cursor = meta_conn.cursor(cursor_factory=extras.RealDictCursor)
	vip_conn = psycopg2.connect(host="localhost", database="vip", user="username", password="password")
	vip_cursor = vip_conn.cursor()
	COPY_SQL_STATEMENT = "COPY {0}({1}) FROM '{2}' WITH CSV HEADER"

	for f in file_data:

		meta_cursor.execute("SELECT hash FROM file_data WHERE file_name = '" + f + "' AND vip_id = " + str(feed_details["vip_id"]) + " AND election_id = " + str(feed_details["election_id"]))
		hash_val = cursor.fetchone()
		new_hash = file_hash(directory + f)
		if not hash_val or new_hash != hash_val:
			r = csv.DictReader(open(directory+f, "r"))
			copy_statement = COPY_SQL_STATEMENT.format(f.split(".")[0].lower(), ",".join(r.fieldnames), "/"+f)
			print copy_statement
			vip_cursor.copy_expert(copy_statement, sys.stdin)
			vip_conn.commit()
			if not hash_val:
				meta_cursor.execute("INSERT INTO file_data (vip_id, election_id, file_name, hash) VALUES (" + str(feed_details["vip_id"]) + "," + str(feed_details["election_id"]) + ",'" + f + "','" + new_hash + "')")
			elif new_hash != hash_val:
				meta_cursor.execute("UPDATE file_data SET hash = " + new_hash + " WHERE vip_id = " + str(feed_details["vip_id"]) + " and election_id = " + str(feed_details["election_id"]))
			os.rename(directory + f, archives + f.split(".")[0] + "_" + file_time_stamp + ".txt")

def convert_to_db_files(feed_details, directory, sp):
	
	error_data = []
	element_counts = {}
	for f in os.listdir(directory):
		element_name, extension = f.lower().split(".")
		with open(directory + f, "r") as reader:
			print "reading " + directory + f
			read_data = csv.DictReader(reader)
			with open(directory + element_name + "_db.txt", "w") as writer:
				dict_fields = sp.header("db", element_name)
				type_vals = sp.type_data("db", element_name)
				if "id" in dict_fields:
					dict_fields.pop(dict_fields.index("id"))
				dict_fields.append("feed_id")
				if not "vip_id" in dict_fields:
					dict_fields.append("vip_id")
				if not "election_id" in dict_fields:
					dict_fields.append("election_id")
				out = csv.DictWriter(writer, fieldnames=dict_fields)
				out.writeheader()
				row_count = 0
				error_count = 0
				for row in read_data:
					row_error = False
					row_count += 1
					for k in row:
						if len(row[k]) <= 0:
							continue
						elif type_vals[k] == "xs:integer":
							try:
								int(row[k])
							except:
								if "id" in row:
									error_data.append({'element_name':element_name,'id':row["id"],'error_details':'Invalid integer given for '+k+':"'+row[k]+'"'})
								row_error = True
						elif type_vals[k] == "xs:string":#this will check for invalid characters
							if row[k].find("<") >= 0 and "id" in row:
								error_data.append({'element_name':element_name,'id':row["id"],'error_details':'Invalid character in string for '+k+':"'+row[k]+'"'})
								row_error = True
						elif type_vals[k] == "xs:date":
							try:
								strptime(row[k],"%Y-%m-%d")
							except:
								if "id" in row:
									error_data.append({'element_name':element_name,'id':row["id"],'error_details':'Invalid date format for '+k+':"'+row[k]+'"'})
								row_error = True
						elif type_vals[k] == "xs:dateTime":
							try:
								strptime(row[k],"%Y-%m-%dT%H:%M:%S")
							except:
								if "id" in row:
									error_data.append({'element_name':element_name,'id':row["id"],'error_details':'Invalid date format for '+k+':"'+row[k]+'"'})
								row_error = True
					if row_error == True:
						error_count += 1
						continue
					if "id" in row:
						row["feed_id"] = row.pop("id")
					row["vip_id"] = feed_details["vip_id"]
					row["election_id"] = feed_details["election_id"]
					out.writerow(row)
				element_counts[element_name] = {'original':row_count,'processed':(row_count-error_count)}
		os.remove(directory + f)
		os.rename(directory + element_name + "_db.txt", directory + f)
		print "finished conversion"
	er.feed_errors(feed_details, error_data)
	return element_counts

def file_hash(fname):
	with open(fname, "rb") as fh:
		m = hashlib.md5()
		for data in fh.read(8192):
			m.update(data)
	return m.hexdigest()

def get_feed_details(directory):

	feed_details = {}
	conn = psycopg2.connect(host="localhost", database="vip_metadata", user="username", password="password")
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
		cursor.execute("INSERT INTO elections (vip_id, election_date, election_type, election_id) VALUES ('" + str(feed_details["vip_id"]) + "','" + feed_details["election_date"] + "','" + feed_details["election_type"] + "','" + str(new_id) + "')")
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
