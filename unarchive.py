import os
import magic
import rarfile
import tarfile
import gzip
import zipfile
import bz2
import xml.sax
import csv
from filetype import FileType

type_mapping = {"gzip":"gz", "bzip2":"bz2", "Zip":"zip", "RAR":"rar", "POSIX tar":"tar"}
type_list = {"compression":["gz", "bz2"], "archived":["zip", "rar", "tar"]}

m = magic.Magic()

class unarchive:
	
	def __init__(self, fname, extract_path=os.getcwd()):

		self.ft = FileType()
	
		if os.path.isfile(fname):
			self.fname = fname
		else:
			print "File does not exist"
			return	

		if not os.path.exists(extract_path):
			os.makedirs(extract_path)
		self.extract_path = extract_path
		
		self.search_dir = self.unpack(self.fname, self.extract_path)

	def get_extract_path(self):
		return self.extract_path

	def get_file_name(self):
		return self.fname

	def get_base_name(self, fname):
	
		clean_name = fname
	
		if clean_name.rfind("/") >= 0:
			clean_name = clean_name[clean_name.rfind("/")+1:]
		if clean_name.find(".") >= 0:
			clean_name = clean_name[:clean_name.find(".")]
	
		return clean_name

	def unpack(self, fname, extract_path=os.getcwd()):
	
		ftype = self.ft.get_type(fname)
		base_name = self.get_base_name(fname)

		if os.path.isdir(fname):
			file_list = os.listdir(fname)
			new_dir = extract_path + "/" + base_name
			for f in file_list:
				self.unpack(new_dir + "/" + f, new_dir)

		if ftype in type_list["compression"]:
		
			if ftype == "gz":
				ext = gzip.GzipFile(fname, 'rb')
			elif ftype == "bz2":
				ext = bz2.BZ2File(fname, 'rb');
		
			filedata = ext.read()
			w = open(extract_path + "/" + base_name,"w")
			w.write(filedata)
			new_type = self.get_type(extract_path + "/" + base_name)
		
			if new_type != "unknown":
				new_name = base_name + "." + new_type
				os.rename(extract_path + "/" + base_name, extract_path + "/" + new_name)
		
				if new_type in type_list["compression"] or new_type in type_list["archived"]:
					self.unpack(extract_path + "/" + new_name, extract_path)

			if fname != self.fname:
				os.remove(fname)

		elif ftype in type_list["archived"]:
		
			if ftype == "rar":
				ext = rarfile.RarFile(fname)
			elif ftype == "tar":
				ext = tarfile.open(fname)
			elif ftype == "zip":
				ext = zipfile.ZipFile(fname)
		
			new_path = extract_path + "/" + base_name + "_extracted"
			if not os.path.exists(new_path):	
				os.makedirs(new_path)
			ext.extractall(path=new_path)
			file_list = os.listdir(new_path)
			for f in file_list:
				self.unpack(new_path + "/" + f, new_path)

			if fname != self.fname:
				os.remove(fname)
		return
	
		
	def find_file_by_name(self, find_file):
		
		return self.find_files_by_name(find_file)[0]
	
	def find_file_by_extension(self, find_extension):

		return self.find_files_by_extension(find_extension)[0]

	def find_file_by_partial(self, find_file):

		return self.find_files_by_partial(find_partial)[0]

	def find_files_by_name(self, find_file):
	
		file_list = []
	
		for root, dirs, dirfiles, in os.walk(self.extract_path):
			for name in dirfiles:
				if find_file == name:
					file_list.append(root + "/" + name)
		
		return file_list
	
	def find_files_by_extension(self, find_extension):
	
		file_list = []	

		if find_extension.startswith("."):
			extension = find_extension
		else:
			extension = "." + find_extension
		
		for root, dirs, dirfiles, in os.walk(self.extract_path):
			for name in dirfiles:
				if name.endswith(extension):
					file_list.append(root + "/" + name)

		return file_list


	def find_files_by_partial(self, find_file):

		file_list = []
	
		for root, dirs, dirfiles, in os.walk(self.extract_path):
			for name in dirfiles:
				if find_file in name:
					file_list.append(root + "/" + name)
		
		return file_list


	def find_folder_by_name(self, find_folder):

		for root, dirs, dirfiles, in os.walk(self.extract_path):
			for dir_name in dirs:
				if find_folder == dir_name:
					return root + "/" + dir_name

	def find_folder_by_partial(self, find_folder):

		for root, dirs, dirfiles, in os.walk(self.extract_path):
			for dir_name in dirs:
				if find_folder in dir_name:
					return root + "/" + dir_name


	def get_unarchive_list(self):
		file_list = []
		
		for root, dirs, dirfiles, in os.walk(self.extract_path):
			file_list.append(root)
			for name in dirfiles:
				path = root + "/" + name
				file_list.append(path)
		
		return file_list
