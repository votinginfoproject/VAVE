import os
import filetype as ft
from hashlib import md5
import directorysearch as ds
import unpack

TEMP_DIR = "temp/"
FEED_DIR = "feed_data/"
ARCHIVE_DIR = "archived/"
SCHEMA_URL = "https://github.com/votinginfoproject/vip-specification/raw/master/vip_spec_v3.0.xsd"
fname = "v2_3.xml"

#pull data in from old config file for cur date, etc?
#also, make sure all directory names passed in are standardized

def main():
	
	setup_dir(TEMP_DIR)

	ftype = ft.get_type(fname)

	ftff = FeedToFlatFiles(TEMP_DIR)

	if ftype == "xml":
		ftff.process_feed(fname)
	elif ft.is_compression(ftype) or ft.is_archived(ftype):
		unpack.unpack(fname, TEMP_DIR)
		unpack.flatten_folder(TEMP_DIR)
		xml_file = ds.file_by_extension(".xml", TEMP_DIR)
		if xml_file:
			ftff.process_feed(xml_file)

	fc = FormatCheck(urlopen(SCHEMA_URL), TEMP_DIR)
	fc.validate_and_clean()
	#need to get error report here

	write_and_archive(fc.get_valid_files(), fc.get_vip_id())

def setup_dir(dir_name):

	if not os.path.isdir(dir_name):
		os.mkdir(dir_name)
	elif dir_name == TEMP_DIR:
		clear_directory(dir_name)

def clear_directory(directory):
	for root, dirs, files in os.walk(directory):
		for f in files:
			os.unlink(os.path.join(root, f))
		for d in dirs:
			rmtree(os.path.join(root, d))

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
