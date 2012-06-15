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

TEMP_DIR = "temp/"
FEED_DIR = "feed_data/"
ARCHIVE_DIR = "archived/"
SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"
CONFIG_FILE = "vip.cfg"
unpack_file = "test.tar.gz"
DEFAULT_ELECTION_ID = 1000

def main():
	
	clear_directory(TEMP_DIR)

	ftype = ft.get_type(unpack_file)

	unpack.unpack(unpack_file, TEMP_DIR)
	unpack.flatten_folder(TEMP_DIR)

	sp = SchemaProps()

	if ds.file_by_name(CONFIG_FILE, TEMP_DIR):
		print "Invalid sections: " + str(process_config(TEMP_DIR, TEMP_DIR + CONFIG_FILE, sp))
	if ds.files_by_extension(".txt", TEMP_DIR) > 0:
		print "Invalid files: " + str(process_flatfiles(TEMP_DIR, sp))
	xml_files = ds.files_by_extension(".xml", TEMP_DIR)
	if len(xml_files) == 1:
		ftff.feed_to_db_files(TEMP_DIR, TEMP_DIR + xml_files[0], sp.full_header_data("db"), sp.version)
		os.remove(TEMP_DIR + xml_files[0])
	
	meta_conn = psycopg2.connect(host="localhost", database="vip_metadata", user="jensen", password="gamet1me")
	meta_cursor = meta_conn.cursor(cursor_factory=extras.RealDictCursor)

	vip_id, election_id = id_vals(TEMP_DIR, meta_cursor, meta_conn)

	process_files(TEMP_DIR, ARCHIVE_DIR, vip_id, election_id, meta_cursor, meta_conn)

def process_files(feed_dir, archive_dir, vip_id, election_id, cursor, conn):

	file_list = os.listdir(self.directory)
	new_files = []
	for f in file_list:
		cursor.execute("SELECT hash FROM file_data WHERE file_name = '" + f + "' AND vip_id = " + str(vip_id) + " AND election_id = " str(election_id))
		hash_val = cursor.fetchone()
		new_hash = file_hash(feed_dir + f)
		if not hash_val:
			cursor.execute("INSERT INTO file_data (vip_id, election_id, file_name, hash) VALUES (" + str(vip_id) + "," + str(election_id) + ",'" + f + "','" + new_hash + "')")
			new_files.append(f)
		if new_hash != hash_val:
			cursor.execute("UPDATE file_data SET hash = " + new_hash + " WHERE vip_id = " str(vip_id) + " and election_id = " + str(election_id))
			new_files.append(f)

	archive_files(feed_dir, archive_dir, new_files)
	
	convert_files(directory, new_files, vip_id, election_id)

	update_db(directory, new_files)

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

	vip_conn = psycopg2.connect(host="localhost", database="vip", user="jensen", password="gamet1me")
	vip_cursor = vip_conn.cursor()
	SQL_STATEMENT = "COPY {0}({1}) FROM '{2}' WITH CSV HEADER"

	for f in files:
		r = csv.DictReader(open(directory+f, "r"))
		copy_statement = SQL_STATEMENT.format(f.split(".")[0].lower(), ",".join(r.fieldnames), "/"+f)
		print copy_statement
		vip_cursor.copy_expert(copy_statement, sys.stdin)
		vip_conn.commit()

def id_vals(directory, cursor, conn):
	with open(directory + "source.txt", "r") as f:
		fdata = csv.DictReader(f)
		for row in fdata:
			vip_id = row["vip_id"]
	with open(directory + "election.txt", "r") as f:
		fdata = csv.DictReader(f)
		for row in fdata:
			election_date = row["date"]
			election_type = row["election_type"]
	
	cursor.execute("SELECT * FROM elections WHERE vip_id = " + str(vip_id) + " AND election_date = '" + election_date + "' AND election_type = '" + election_type + "'")
	election_data = cursor.fetchone()
	if not election_data:
		cursor.execute("SELECT GREATEST(election_id) FROM elections")
		last_id = cursor.fetchone()
		if not last_id:
			new_id = DEFAULT_ELECTION_ID
		else:
			new_id = int(last_id) + 1
		cursor.execute("INSERT INTO elections (vip_id, election_date, election_type, election_id) VALUES (" + str(vip_id) + "," + election_date + "," + election_type + "," + str(new_id) + ")")
		conn.commit()
		return vip_id, election_id
	else:
		return vip_id, election_data["election_id"]

	
def clear_directory(directory):

	if not os.path.exists(directory):
		os.mkdir(directory)
		return

	for root, dirs, files in os.walk(directory):
		for f in files:
			os.unlink(os.path.join(root, f))
		for d in dirs:
			rmtree(os.path.join(root, d))

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
				convert_data(directory, k, v, schema_props.conversion_by_element(v))
	return invalid_files

def convert_data(directory, fname, element, conversion_dict):
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
	with open(directory + fname, "r") as f:
		fdata = csv.DictReader(f)
		header = fdata.fieldnames
		output_list = []
		header_list = []
		for h in header:
			if h in element_conversion:
				#need to figure out which (key or value) is being pulled for the header name
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

def write_and_archive(valid_files, vip_id):
	
	feed_dir = FEED_DIR + vip_id
	setup_dir(feed_dir)
	archive_dir = ARCHIVE_DIR + vip_id
	setup_dir(archive_dir)
	
	file_list = os.listdir(self.directory)
	
	for fname in valid_files:
		if fname in file_list:
			if file_hash(TEMP_DIR + fname) == file_hash(feed_dir + fname):
				continue 
			else:
				date_created = os.stat(feed_dir + fname).st_ctime
				os.rename(feed_dir + fname, archive_dir + fname[:fname.rfind(".")] + "_" + str(int(os.path.getctime(feed_dir+fname))) + ".txt")
				os.rename(TEMP_DIR + fname, feed_dir + fname)
		else:
			os.rename(TEMP_DIR + fname, feed_dir + fname)

def file_hash(fname):
	with open(fname, "rb") as fh:
		m = md5()
		for data in fh.read(8192):
			m.update(data)
	return m.hexdigest()

if __name__ == "__main__":
	main()
