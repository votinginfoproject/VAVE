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
import sys
import re

DIRECTORIES = {"temp":"/tmp/temp/", "archives":"/tmp/archives/"}
SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"
CONFIG_FILE = "vip.cfg"
unpack_file = "test.zip"
DEFAULT_ELECTION_ID = 1000
REQUIRED_FILES = ["source.txt", "election.txt"]
process_time = str(datetime.now())
file_time_stamp = process_time[:process_time.rfind(".")].replace(":","-").replace(" ","_")
LOCALITY_TYPES = ['county','city','town','township','borough','parish','village','region']
ZIPCODE_REGEX = re.compile("\d{5}(?:[-\s]\d{4})?")
EMAIL_REGEX = re.compile("[a-zA-Z0-9+_\-\.]+@[0-9a-zA-Z][.-0-9a-zA-Z]*.[a-zA-Z]")
URL_REGEX = re.compile("http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))")
PHONE_REGEX = re.compile("1?\s*\W?\s*([2-9][0-8][0-9])\s*\W?\s*([2-9][0-9]{2})\s*\W?\s*([0-9]{4})(\se?x?t?(\d*))?")
VALID_DIRECTIONS = ['n','s','e','w','nw','ne','sw','se','north','south','east','west','northeast','northwest','southeast','southwest']

def main():

	print "setting up directories..."
	
	dt.clear_or_create(DIRECTORIES["temp"])
	dt.create_directory(DIRECTORIES["archives"])
	
	print "done setting up directories"

	ftype = ft.get_type(unpack_file)

	print "unpacking and flattening files..."

	unpack.unpack(unpack_file, DIRECTORIES["temp"])
	unpack.flatten_folder(DIRECTORIES["temp"])
# I could have flatten_folder return a list of files in the directory, so that
# we wouldn't have to search through the directory everytime for specific files
# since os.walk is slow with directories with large files

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
	if len(xml_files) >= 1:
		ftff.feed_to_db_files(DIRECTORIES["temp"], xml_files[0], sp.full_header_data("db"), sp.version)
		os.remove(xml_files[0])
		valid_files.append(xml_files[0])

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

	print "done converting to full db files"

	update_data(feed_details, element_counts, DIRECTORIES["temp"], DIRECTORIES["archives"])	

	er.e_count_summary(feed_details, element_counts)

	db_validations(feed_details, sp)

	generate_feed(feed_details)

def db_validations(feed_details, sp):
	conn = psycopg2.connect(host="localhost", database="vip", user="username", password="password")
	cursor = conn.cursor(cursor_factory=extras.ReadDictCursor)
	table_columns = sp.full_header_data("db")
	tables = header_data.keys()
	duplicate_data = []
	error_data = []
	warning_data = []
	for t in tables:
		if t == "street_segment":
			continue
		query = "SELECT t1.feed_id AS 'id', t2.feed_id AS 'duplicate_id' FROM " + t + " t1, " + t + " t2 WHERE t1.election_id = " + str(feed_details["election_id"]) + " AND t2.election_id = " + str(feed_details["election_id"]) + " AND t1.vip_id = " + str(feed_details["vip_id"]) + " AND t2.vip_id = " + str(feed_details["vip_id"]) + " AND t1.feed_id != t2.feed_id "
		for column in table_columns[t]:
			if column == "id":
				continue
			query += " AND t1." + column + " = t2." + column
		cursor.execute(query)
		duplicates = cursor.fetchall()
		for d in duplicates:
			duplicate_data.append({"element_name":t,"id":d['id'],"duplicate_id":d['duplicate_id']})
		for column in table_columns[t]:
			if column.endswith("_id") and column[:-3] in tables:
				query = "SELECT " + column + ", feed_id FROM " + t + " WHERE election_id = " + str(feed_details["election_id"]) + " AND vip_id = " + str(feed_details["vip_id"]) + " AND " + column + " NOT IN (SELECT feed_id FROM " + column[:-3] + " WHERE vip_id = " + str(feed_details["vip_id"]) + " AND election_id = " + str(feed_details["election_id"])
				cursor.execute(query)
				missing_ids = cursor.fetchall()
				for m in missing_ids:
					error_data.append({'base_element':t,'problem_element':column,'id':m["feed_id"],'error_details':'Missing element mapping, ' + column + ' with id of ' + m[column] + ' does not exist'})
	query = "SELECT feed_id FROM street_segments WHERE start_house_number > end_house_number"
	cursor.execute(query)
	bad_house_numbers = cursor.fetchall()
	for house in bad_house_numbers:
		error_data.append({'base_element':'street_segment','id':m["feed_id"],'problem_element':'start-end_house_number','error_details':'Starting house numbers must be less than ending house numbers'})
	query = "SELECT feed_id from street_segments WHERE election_id = " + str(feed_details["election_id"]) + " AND vip_id = " + str(feed_details["vip_id"]) + " AND odd_even_both LIKE 'odd' AND (mod(start_house_number,2) = 0 OR mod(end_house_number,2) = 0)"
	cursor.execute(query)
	odd_mismatch = cursor.fetchall()
	for odd in odd_mismatch:
		warning_data.append({'base_element':'street_segment','id':m["feed_id"],'problem_element':'odd_even_both','error_details':'Start and ending house numbers should be odd when odd_even_both is set to odd'})
	query = "SELECT feed_id from street_segments WHERE election_id = " + str(feed_details["election_id"]) + " AND vip_id = " + str(feed_details["vip_id"]) + " AND odd_even_both LIKE 'even' AND (mod(start_house_number,1) = 0 OR mod(end_house_number,1) = 0)"
	cursor.execute(query)
	even_mismatch = cursor.fetchall()
	for even in even_mismatch:
		warning_data.append({'base_element':'street_segment','id':m["feed_id"],'problem_element':'odd_even_both','error_details':'Start and ending house numbers should be even when odd_even_both is set to even'})
	query = "SELECT s1.feed_id, s1.start_house_number, s1.end_house_number, s1.odd_even_both, s1.precinct_id, s2.feed_id, s2.start_house_number, s2.end_house_number, s2.odd_even_both, s2.precinct_id FROM street_segment s1, street_segment s2 WHERE s1.election_id = " + str(feed_details["election_id"]) + " AND s1.vip_id = " + str(feed_details["vip_id"]) + " AND s2.election_id = s1.election_id AND s2.vip_id = s1.vip_id AND s1.feed_id != s2.feed_id AND s1.start_house_number BETWEEN s2.start_house_number AND s2.end_house_number AND s1.odd_even_both = s2.odd_even_both AND ((s1.non_house_address_street_direction IS NULL AND s2.non_house_address_street_direction IS NULL) OR s1.non_house_address_street_direction = s2.non_house_address_street_direction) AND ((s1.non_house_address_street_suffix IS NULL AND s2.non_house_address_street_suffix IS NULL) OR s1.non_house_address_street_suffix = s2.non_house_address_street_suffix) AND s1.non_house_address_street_name = s2.non_house_address_street_name AND s1.non_house_address_city = s2.non_house_address_city AND s1.non_house_address_state = s2.non_house_address_state AND s1.non_house_address_zip = s2.non_house_address_zip"
	cursor.execute(query)
	segment_issues = cursor.fetchall()
	
	er.feed_issues(feed_details, error_data, "error")
	er.feed_issues(feed_details, warning_data, "warning")
	er.feed_issues(feed_details, duplicate_date, "duplicate")

	
	

def update_data(feed_details, element_counts, directory, archives):

	meta_conn = psycopg2.connect(host="localhost", database="vip_metadata", user="username", password="password")
	meta_cursor = meta_conn.cursor(cursor_factory=extras.RealDictCursor)
	vip_conn = psycopg2.connect(host="localhost", database="vip", user="username", password="password")
	vip_cursor = vip_conn.cursor()
	COPY_SQL_STATEMENT = "COPY {0}({1}) FROM '{2}' WITH CSV HEADER"

	for f in os.listdir(directory):
		element_name, extension = f.lower().split(".")
		meta_cursor.execute("SELECT hash FROM file_data WHERE file_name = '" + f + "' AND vip_id = " + str(feed_details["vip_id"]) + " AND election_id = " + str(feed_details["election_id"]))
		result = meta_cursor.fetchone()
		if result:
			hash_val = result["hash"]
		else:
			hash_val = None
		new_hash = file_hash(directory + f)
		if not hash_val:
			r = csv.DictReader(open(directory+f, "r"))
			copy_statement = COPY_SQL_STATEMENT.format(element_name, ",".join(r.fieldnames), "/tmp/temp/"+ f)
			print "upload to database " + element_name + " data"
			vip_cursor.copy_expert(copy_statement, sys.stdin)
			vip_conn.commit()
			meta_cursor.execute("INSERT INTO file_data (vip_id, election_id, file_name, hash) VALUES (" + str(feed_details["vip_id"]) + "," + str(feed_details["election_id"]) + ",'" + f + "','" + new_hash + "')")
			meta_conn.commit()
			meta_cursor.execute("INSERT INTO feed_data (vip_id, election_id, element, original_count, final_count) VALUES ('" + str(feed_details["vip_id"]) + "','" + str(feed_details["election_id"]) + "','" + element_name + "','" + str(element_counts[element_name]["original"]) + "','" + str(element_counts[element_name]["processed"]) + "')")
			meta_conn.commit()
			os.rename(directory + f, archives + element_name + "_" + file_time_stamp + ".txt")
		if new_hash != hash_val:
			#might configure postgres to partition based on vip/election id and then drop partition,
			#or write a query to join valid data into a new table and rename
			vip_cursor.execute("DELETE FROM " + element_name + " WHERE vip_id = " + str(feed_details["vip_id"]) + " AND election_id = " + str(feed_details["election_id"]))
			vip_conn.commit()
			r = csv.DictReader(open(directory+f, "r"))
			copy_statement = COPY_SQL_STATEMENT.format(element_name, ",".join(r.fieldnames), "/tmp/temp/"+ f)
			print "upload to database " + element_name + " data"
			vip_cursor.copy_expert(copy_statement, sys.stdin)
			vip_conn.commit()
			meta_cursor.execute("UPDATE file_data SET hash = " + new_hash + " WHERE vip_id = " + str(feed_details["vip_id"]) + " and election_id = " + str(feed_details["election_id"]))
			meta_conn.commit()
			meta_cursor.execute("UPDATE feed_data SET original_count = '" + str(element_counts[element_name]["original"]) + "' AND final_count = '" + str(element_counts[element_name]['processed']) + "' WHERE vip_id = " + str(feed_details["vip_id"]) + " AND election_id = " + str(feed_details["election_id"]) + " AND element LIKE '" + element_name + "'")
			meta_conn.commit()
			os.rename(directory + f, archives + element_name + "_" + file_time_stamp + ".txt")

def convert_to_db_files(feed_details, directory, sp):

	feed_ids = {}
	error_data = []
	warning_data = []
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
				processed_count = 0
				for row in read_data:
					row_error = False
					row_count += 1
					for k in row:
						error = validate(element_name, k, type_vals[k]["type"], row, type_vals[k]["is_required"])
						if "error_details" in error:
							error_data.append(error)
						elif "warning_details" in error:
							warning_data.append(error)
					if "id" in row:
						row["feed_id"] = row.pop("id")
						if element_name == "source":
							row["feed_id"] = 1
						elif element_name == "election":
							row["feed_id"] = feed_details["election_id"]
						if row["feed_id"] not in feed_ids:
							feed_ids[row["feed_id"]] = None
						else:
							error_data.append({'base_element':element_name,'error_element':'id','id':row["feed_id"],'error_details':'Element ID is not unique to the feed'})
							continue
					row["vip_id"] = feed_details["vip_id"]
					row["election_id"] = feed_details["election_id"]
					out.writerow(row)
					processed_count += 1
				element_counts[element_name] = {'original':row_count,'processed':processed_count}
		os.remove(directory + f)
		os.rename(directory + element_name + "_db.txt", directory + f)
		print "finished conversion"
	er.feed_issues(feed_details, error_data, "error")
	er.feed_issues(feed_details, warning_data, "warning")
	return element_counts

def validate(e_name, key, xml_type, row, required):

	error_dict = {'base_element':e_name, 'problem_element':key}
	if "id" in row:
		error_dict["id"] = row["id"]
	else:
		error_dict["id"] = 'xxx'

	if len(row[key]) <= 0 or row[key].lower() in ["none","n/a","-","na"]: 
		if required == "true":
			error_dict["error_details"] = 'Missing required value' 
	elif xml_type == "xs:integer":
		try:
			int(row[key])
			if key == "end_apartment_number" and int(row[key]) == 0:
				error_dict["error_details"] = 'Ending apartment number must be greater than zero:"'+row[key]+'"'
			elif key == "end_house_number" and int(row[key]) == 0:
				error_dict["error_details"] = 'Ending house number must be greater than zero:"'+row[key]+'"'
		except:
			error_dict["error_details"] = 'Invalid integer given:"'+row[key]+'"'
	elif xml_type == "xs:string":
		if row[key].find("<") >= 0: #need to add in other invalid character checks
			error_dict["error_details"] = 'Invalid character in string:"'+row[key]+'"'
		elif key == "zip" and not ZIPCODE_REGEX.match(row[key]):
			error_dict["warning_details"] = 'Invalid Zip:"'+row[key]+'"'
		elif key == "email" and not EMAIL_REGEX.match(row[key]):
			error_dict["warning_details"] = 'Invalid Email:"'+row[key]+'"'
		elif key.endswith("_url") and not URL_REGEX.match(row[key]):
			error_dict["warning_details"] = 'Invalid URL:"'+row[key]+'"'
		elif key == "state" and len(row[key]) != 2:
			error_dict["warning_details"] = 'Invalid state abbreviation:"'+row[key]+'"'
		elif key == "locality" and row[key].lower() not in LOCALITY_TYPES:
			error_dict["error_details"] = 'Invalid type:"'+row[key]+'"'
		elif (key == "phone" or key == "fax") and not PHONE_REGEX.match(row[key].lower()):
			error_dict["warning_details"] = 'Invalid phone:"'+row[key]+'"' 
		elif key.endswith("_direction") and row[key].lower().replace(' ','') not in VALID_DIRECTIONS:
			error_dict["error_details"] = 'Invalid direction:"'+row[key]+'"'
		elif key.find("hours") >= 0 and (row[key].find("to") >= 0 or row[key].find("-") >= 0):#can be improved, just a naive check to make sure there is some hour range value
			error_dict["warning_details"] = 'No hour range provided:"'+row[key]+'"'
	elif xml_type == "xs:date":
		try:
			strptime(row[key],"%Y-%m-%d")
		except:
			error_dict["error_details"] = 'Invalid date format for '+key+':"'+row[key]+'"'
	elif xml_type == "xs:dateTime":
		try:
			strptime(row[key],"%Y-%m-%dT%H:%M:%S")
		except:
			error_dict["error_details"] = 'Invalid date format for '+key+':"'+row[key]+'"'
	elif xml_type == 'yesNoEnum':
		if row[key].lower() not in ['yes','no']:
			error_dict["error_details"] = 'Must be "yes" or "no"'
	elif xml_type == 'oebEnum':
		if row[key].lower() not in ['odd','even','both']:
			error_dict["error_details"] = 'Must be "odd", "even", or "both"'
	return error_dict

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
	details_dict = {'source.txt':['vip_id','name','datetime'],
			'election.txt':['date','election_type']}
	
	try:
		for fname in details_dict:
			with open(directory + fname, "r") as f:
				reader = csv.DictReader(f)
				row = reader.next()
				for val in details_dict[fname]:
					feed_details[val] = row[val] 
	except:
		return feed_details
	
	cursor.execute("SELECT * FROM elections WHERE vip_id = " + str(feed_details["vip_id"]) + " AND election_date = '" + feed_details["date"] + "' AND election_type = '" + feed_details["election_type"] + "'")
	election_data = cursor.fetchone()
	if not election_data:
		cursor.execute("SELECT GREATEST(election_id) FROM elections")
		last_id = cursor.fetchone()
		if not last_id:
			new_id = DEFAULT_ELECTION_ID
		else:
			new_id = int(last_id["greatest"]) + 1
		cursor.execute("INSERT INTO elections (vip_id, election_date, election_type, election_id) VALUES ('" + str(feed_details["vip_id"]) + "','" + feed_details["date"] + "','" + feed_details["election_type"] + "','" + str(new_id) + "')")
		conn.commit()
		feed_details["election_id"] = new_id
	else:
		feed_details["election_id"] = election_data["election_id"]
	return feed_details

#add in header to all valid formatted files, delete invalid files
def process_config(directory, config_file, sp):
	
	config = ConfigParser()
	config.read(config_file)
	sections = config.sections()
	
	db_or_element = db_or_element_format(sp, sections)
	
	if db_or_element == "db":
		invalid_sections = fc.invalid_config_sections(directory, config_file, sp.full_header_data("db"))
	elif db_or_element == "element":
		invalid_sections = fc.invalid_config_sections(directory, config_file, sp.full_header_data("element"))
	else:
		return "error"

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

#similar to directory tools search except this returns a file/element dict
#and uses listdir on a flattened folder instead of the slower os.walk
def files_ename_by_extension(directory, extension):
	f_list = {}
	for f in os.listdir(directory):
		element_name, f_exten = f.lower()split(".")
		if f_exten == extension:
			f_list[f] = element_name
	return f_list

def db_or_element_format(sp, check_list):
	if any(vals not in sp.key_list("element") for vals in check_list):
		if all(vals in sp.key_list("db") for vals in check_list):
			return "db"
		else:
			return "error"
	else:
		return "element"

#check flat file format, if element format, check then convert to element format
#if db format, check and then leave alone
def process_flatfiles(directory, sp):

	file_list = files_ename_by_extension(directory, "txt")

	db_or_element = db_or_element_format(sp, file_list.values()
	
	if db_or_element == "db":
		invalid_files = fc.invalid_files(directory, file_list, sp.full_header_data("db"))
	elif db_or_element == "element":
		invalid_files = fc.invalid_files(directory, file_list, sp.full_header_data("element"))
		for k, v in file_list.iteritems():
			if k not in invalid_files:
				valid_files = convert_data(directory, k, v, sp.conversion_by_element(v))
	else:
		return "error"

	for f in invalid_files:
		os.remove(directory + f)

	return invalid_files

#converts data from element format to db format. Currently opens and reads
#through the whole file each time, splitting on each row was significantly slower
def convert_data(directory, fname, element, conversion_dict):
	for conversion in conversion_dict:
		with open(directory + fname, "r") as f:
			if conversion == element:
				output = directory + element + "_temp.txt"
			else:
				output = directory + conversion + ".txt"
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
				with open(output, "w") as w:
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
