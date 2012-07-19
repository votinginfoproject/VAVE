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
import csv
from shutil import copyfile
import hashlib
from datetime import datetime
from time import strptime
import re
from easysql import EasySQL

DIRECTORIES = {"temp":"/tmp/temp/", "archives":"/tmp/archives/"}
CONFIG_FILE = "vip.cfg"
unpack_file = "test.zip"
DEFAULT_ELECT_ID = 1000
SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"
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

	sp = SchemaProps(SCHEMA_URL)
	file_details = {"file":unpack_file, "process_time":process_time, "file_time_stamp":file_time_stamp}
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
	db = EasySQL("localhost","vip","username","password")
	feed_details = {}
	try:
		with open(DIRECTORIES["temp"] + "source.txt", "r") as f:
			reader = csv.DictReader(f)
			row = reader.next()
			feed_details["vip_id"] = row["vip_id"]
		with open(DIRECTORIES["temp"] + "election.txt", "r") as f:
			reader = csv.DictReader(f)
			row = reader.next()
			feed_details["election_date"] = row["date"]
			feed_details["election_type"] = row["election_type"]
	except:
		er.report_summary(feed_details, file_details, valid_files, invalid_files, invalid_sections)
		return
	er.report_setup(feed_details["vip_id"])

	election_id = get_election_id(feed_details, db)
	print "done getting feed details"

	print "converting to full db files...."
	element_counts = convert_to_db_files(feed_details["vip_id"], election_id, file_time_stamp, DIRECTORIES["temp"], sp)

	print "done converting to full db files"

	update_data(feed_details["vip_id"], election_id, db, element_counts, DIRECTORIES["temp"], DIRECTORIES["archives"])	

	er.e_count_summary(feed_details, element_counts)

	db_validations(feed_details, sp)

	generate_feed(feed_details)

def get_election_id(feed_details, db):
	table_list = ['meta_elections']
	result = db.select(table_list,['election_id'],feed_details,1)
	if not result:
		last_id = db.select(table_list,['GREATEST(election_id)'],None,1)
		if not last_id:
			new_id = DEFAULT_ELECT_ID
		else:
			new_id = int(last_id["greatest"]) + 1
		feed_details["election_id"] = str(new_id)
		print feed_details
		db.insert(table_list[0],[feed_details])
	else:
		return str(result["election_id"])
	return str(feed_details["election_id"])

def update_data(vip_id, election_id, db, element_counts, directory, archives):

	file_list = files_ename_by_extension(directory, "txt")
	for k, v in file_list.iteritems():
		result = db.select(['meta_file_data'],['hash'],{'file_name':k,'vip_id':vip_id,'election_id':election_id},1)
		hash_val = None
		full_path = directory + k
		if result:
			hash_val = result["hash"]
		new_hash = file_hash(full_path)
		if not hash_val:
			r = csv.DictReader(open(full_path, "r"))
			db.copy_upload(v, r.fieldnames, full_path)
			db.insert('meta_file_data',[{'file_name':k,'vip_id':vip_id,'election_id':election_id,'hash':new_hash}])
			db.insert('meta_feed_data',[{'element':v,'vip_id':vip_id,'election_id':election_id,'element_count':element_counts[v]}])
			os.rename(full_path,archives+v+"_"+file_time_stamp+".txt")
		elif new_hash != hash_val:
			db.delete(v,{'vip_id':vip_id,'election_id':election_id})
			r = csv.DictReader(open(full_path, "r"))
			db.copy_upload(e_name, r.fieldnames, full_path)
			db.update('meta_file_data',{'hash':new_hash},{'file_name':k,'vip_id':vip_id,'election_id':election_id})
			db.update('meta_feed_data',{'element_count':element_count},{'element':v,'vip_id':vip_id,'election_id':election_id})
			os.rename(full_path,archives+v+"_"+file_time_stamp+".txt")

def db_validations(vip_id, election_id, db, feed_details, sp):
	table_columns = sp.full_header_data("db")
	tables = header_data.keys()
	duplicate_data = []
	error_data = []
	warning_data = []
	for t in tables:
		if t == "street_segment":
			continue
		base_conditions = {'election_id':{'condition':'=','compare_to':election_id},'vip_id':{'condition':'=','compare_to':vip_id}}
		for column in table_columns[t]:
			if column != "id":
				join_comparisons[column] = '='
		results = db.leftjoin(t, ['feed_id AS "id"'], base_conditions, t, ['feed_id AS "duplicate_id"'], {}, join_comparisons)
		for d in results:
			duplicate_data.append({"element_name":t,"id":d['id'],"duplicate_id":d['duplicate_id']})
		for column in table_columns[t]:
			if column.endswith("_id") and column[:-3] in tables:
				results = db.custom_query("SELECT " + column + ", feed_id FROM " + t + " WHERE election_id = " + str(election_id) + " AND vip_id = " + str(vip_id) + " AND " + column + " NOT IN (SELECT feed_id FROM " + column[:-3] + " WHERE vip_id = " + str(vip_id) + " AND election_id = " + str(election_id))
				for m in results:
					error_data.append({'base_element':t,'problem_element':column,'id':m["feed_id"],'error_details':'Missing element mapping, ' + column + ' with id of ' + m[column] + ' does not exist'})
	
	bad_house_numbers = db.select(['street_segments'],['feed_id'],{'election_id':election_id,'vip_id':vip_id,'start_house_number':{'condition':'>','compare_to':'end_house_number'}})
	for house in bad_house_numbers:
		error_data.append({'base_element':'street_segment','id':m["feed_id"],'problem_element':'start-end_house_number','error_details':'Starting house numbers must be less than ending house numbers'})
	results = db.custom_query("SELECT feed_id from street_segments WHERE election_id = " + str(election_id) + " AND vip_id = " + str(vip_id) + " AND odd_even_both LIKE 'odd' AND (mod(start_house_number,2) = 0 OR mod(end_house_number,2) = 0)")
	for odd in results:
		warning_data.append({'base_element':'street_segment','id':m["feed_id"],'problem_element':'odd_even_both','error_details':'Start and ending house numbers should be odd when odd_even_both is set to odd'})
	results = db.custom_query("SELECT feed_id from street_segments WHERE election_id = " + str(election_id) + " AND vip_id = " + str(vip_id) + " AND odd_even_both LIKE 'even' AND (mod(start_house_number,1) = 0 OR mod(end_house_number,1) = 0)")
	for even in results:
		warning_data.append({'base_element':'street_segment','id':m["feed_id"],'problem_element':'odd_even_both','error_details':'Start and ending house numbers should be even when odd_even_both is set to even'})
	results = db.custom_query("SELECT s1.feed_id, s1.start_house_number, s1.end_house_number, s1.odd_even_both, s1.precinct_id, s2.feed_id, s2.start_house_number, s2.end_house_number, s2.odd_even_both, s2.precinct_id FROM street_segment s1, street_segment s2 WHERE s1.election_id = " + str(election_id) + " AND s1.vip_id = " + str(vip_id) + " AND s2.election_id = s1.election_id AND s2.vip_id = s1.vip_id AND s1.feed_id != s2.feed_id AND s1.start_house_number BETWEEN s2.start_house_number AND s2.end_house_number AND s1.odd_even_both = s2.odd_even_both AND s1.non_house_address_street_direction IS NOT DISTICT FROM s2.non_house_address_street_direction AND s1.non_house_address_street_suffix IS NOT DISTICT FROM s2.non_house_address_street_suffix AND s1.non_house_address_street_name = s2.non_house_address_street_name AND s1.non_house_address_city = s2.non_house_address_city AND s1.non_house_address_state = s2.non_house_address_state AND s1.non_house_address_zip = s2.non_house_address_zip")
	
	if len(error_data) > 0:
		er.feed_issues(feed_details, error_data, "error")
	if len(warning_data) > 0:
		er.feed_issues(feed_details, warning_data, "warning")
	er.feed_issues(feed_details, duplicate_date, "duplicate")

def convert_to_db_files(vip_id, election_id, file_time_stamp, directory, sp):

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
							row["feed_id"] = election_id
						if row["feed_id"] not in feed_ids:
							feed_ids[row["feed_id"]] = None
						else:
							error_data.append({'base_element':element_name,'error_element':'id','id':row["feed_id"],'error_details':'Element ID is not unique to the feed'})
							continue
					row["vip_id"] = vip_id
					row["election_id"] = election_id
					out.writerow(row)
				element_counts[element_name] = str(row_count)
		os.remove(directory + f)
		os.rename(directory + element_name + "_db.txt", directory + f)
		print "finished conversion"
	if len(error_data) > 0:
		er.feed_issues(vip_id, file_time_stamp, error_data, "error")
	if len(warning_data) > 0:
		er.feed_issues(vip_id, file_time_stamp, warning_data, "warning")
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
		element_name, f_exten = f.lower().split(".")
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

	db_or_element = db_or_element_format(sp, file_list.values())
	
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
