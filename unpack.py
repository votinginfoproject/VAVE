import os
import shutil
from filetype import FileType
from gzip import GzipFile
from bz2 import BZ2File
from rarfile import RarFile
from zipfile import ZipFile
import tarfile
import re

ft = FileType()

class Unpack:

	def __init__(self, file_name, extract_path=None):

		self.ft = FileType()

		self.extract_path = extract_path

		if os.path.exists(file_name) and self.extract_path:
			os.makedirs(self.extract_path)

		if os.path.isdir(file_name) and self.extract_path:
			shutil.copytree(file_name, extract_path)
		elif os.path.isfile(file_name) and self.extract_path:
			shutil.copy(file_name, extract_path)
		elif os.path.isdir(file_name):
			self.extract_path = file_name
		else:
			self.extract_path = os.getcwd()

		if os.path.isdir(file_name):
			self.unpack_dir(file_name)
		else:
			self.unpack_file(file_name)

	def uncompress(self, fname):
		ftype = self.ft.get_type(fname)
	
		if ftype == "gz":
			ext = GzipFile(fname, 'rb')
		elif ftype == "bz2":
			ext = BZ2File(fname, 'rb')

		filedata = ext.read()
		new_name = fname[:fname.rfind(".")]
		w = open(new_name, "w")
		w.write(filedata)

		new_type = self.ft.get_type(new_name)
		if new_type:
			os.rename(new_name, new_name + "." + new_type)
			return new_name + "." + new_type
		return new_name

	def unarchive(self, fname):
		ftype = self.ft.get_type(fname)
	
		if ftype == "rar":
			ext = RarFile(fname)
		elif ftype == "tar":
			ext = tarfile.open(fname)
		elif ftype == "zip":
			ext = ZipFile(fname)

		new_path = fname[:fname.rfind(".")] + "_extracted"
		if not os.path.exists(new_path):
			os.makedirs(new_path)
		ext.extractall(path=new_path)
		return new_path

	def unpack_dir(self, directory):
		for root, dirs, dirfiles in os.walk(directory):
			for name in dirfiles:
				full_name = root + "/" + name
				self.unpack_file(full_name)

	def unpack_file(self, fname):
		if self.ft.is_compression(fname) or self.ft.is_archived(fname):
		
			if self.ft.is_compression(fname):
				new_file = self.uncompress(fname)
			elif self.ft.is_archived(fname):
				new_file = self.unarchive(fname)
		
			if fname != new_file:
				os.remove(fname)
		
			if os.path.isdir(new_file):
				self.unpack_dir(new_file)
			else:
				self.unpack_file(new_file)

	def find_file_by_name(self, file_name):
		
		return self.find_files_by_name(file_name)[0]

	def find_files_by_name(self, file_name):
		return self.find_files(re.compile(file_name))
	
	def find_file_by_extension(self, file_extension):

		return self.find_files_by_extension(file_extension)[0]

	def find_files_by_extension(self, file_extension):
		
		if not file_extension.startswith("."):
			file_extension = "." + file_extension
		return self.find_files(re.compile(".*\\" + file_extension))

	def find_file_by_partial(self, file_partial):

		return self.find_files_by_partial(file_partial)[0]

	def find_files_by_partial(self, file_partial):
		
		return self.find_files(re.compile(".*" + file_partial + ".*"))

	def find_files(self, regex):
		
		file_list = []

		for root, dirs, dirfiles in os.walk(self.extract_path):
			for name in dirfiles:
				if regex.match(name) and name.find("/.") < 0:
					file_list.append(root + "/" + name)
		if len(file_list) > 0:
			return file_list

	def find_folder_by_name(self, folder_name):

		return self.find_folder(re.compile(folder_name))

	def find_folder_by_partial(self, folder_partial):
		
		return self.find_folder(re.compile(".*" + folder_partial + ".*"))

	def find_folder(self, regex):

		for root, dirs, dirfiles in os.walk(self.extract_path):
			for dir_name in dirs:
				if regex.match(dir_name) and dir_name.find("/.") < 0:
					return root + "/" + dir_name
	def get_file_list(self):
		
		file_list = []
		
		for root, dirs, dirfiles in os.walk(self.extract_path):
			for name in dirfiles:
				file_list.append(root + "/" + name)
		
		if len(file_list) > 0:
			return file_list

#TODO:Write def flatten_folder():
	
if __name__ == '__main__':
	unpack = Unpack("directory1")
	print unpack.find_files_by_extension(".xml")
	print unpack.find_files_by_partial("state")
	print unpack.find_files_by_name("wtf.txt")
