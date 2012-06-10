import os
from hashlib import md5
import filetype as ft
import directorysearch as ds
import unpack
import formatcheck as fc
from schemaprops import SchemaProps
from ConfigParser import ConfigParser

TEMP_DIR = "temp/"
FEED_DIR = "feed_data/"
ARCHIVE_DIR = "archived/"
SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"
CONFIG_FILE = "vip.cfg"
fname = "v2_3.xml"

def main():
	
	clear_directory(TEMP_DIR)

	ftype = ft.get_type(fname)

	unpack.unpack(fname, TEMP_DIR)
	unpack.flatten_folder(TEMP_DIR)

	sp = SchemaProps()

	if ds.file_by_name(CONFIG_FILE, TEMP_DIR):
		process_config(TEMP_DIR, TEMP_DIR + CONFIG_FILE, sp)
	if ds.files_by_extension(".txt", TEMP_DIR) > 0:
		process_flatfiles(TEMP_DIR)
	xml_files = ds.files_by_extension(".xml", TEMP_DIR)
	if len(xml_files) == 1:
		ftff.process_feed(xml_files[0])
	
	#need to get error report here
	#write_and_archive(fc.get_valid_files(), fc.get_vip_id())

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
						w.write(r)
				os.remove(directory + fname)
			os.rename(directory + s + "_temp.txt", directory + fname)
	os.remove(config_file)
	return invalid_sections

#check flat file format, if element format, check then convert to element format
#if db format, check and then leave alone
def process_flatfiles(directory, schema_props):

	file_list = {} 
	for f in os.listdir(directory):
		fname, extension = f.lower().split(".")
		if extension == "txt" or extension == "csv":
			file_list[fname] = f

	if any(fname not in schema_props.key_list("element") for fname in file_list.keys()):
		if all(fname in schema_props.key_list("db") for fname in file_list.keys()):
			invalid_files = fc.invalid_files(directory, file_list, schema_props.full_header_data("db"))
		else:
			print "sections error!!!"
	else:
		invalid_files = fc.invalid_files(directory, file_list, schema_props.full_header_data("element"))
		#convert files that are not on the invalid files list into db files

	for invalid in invalid_files:
		os.remove(directory + invalid)
	return invalid_files

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
