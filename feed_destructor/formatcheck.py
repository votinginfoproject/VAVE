import csv
from os import path
from ConfigParser import ConfigParser

def invalid_config_sections(directory, config_file, section_props):
	
	config = ConfigParser()
	config.read(config_file)
	sections = config.sections()
	invalid_sections = []
	for s in sections:
		if s not in section_props:
			invalid_sections.append(s)
		elif not config.has_option(s, "file_name") or not config.has_option(s, "header"):
			invalid_sections.append(s)
		elif not os.path.exists(directory + config.get(s, "file_name")):
			invalid_sections.append(s)
		else:
			header = config.get(s, "header").split(",")
			if any(h not in section_props[s] for h in header):
				invalid_sections.append(s)
				continue
			with open(directory + config.get(s, "file_name")) as f:
				fdata = csv.reader(f)
				try:
					if len(fdata.next()) != len(fieldnames):
						invalid_sections.append(s)
				except:
					invalid_sections.append(s)
	return invalid_sections

def invalid_files(directory, file_list, file_props):
	invalid_files = []
 	for k, v in file_list:
		with open(directory + k) as f:
			try:
				fdata = csv.DictReader(f)
			except:
				invalid_files.append(f)
				continue
			if any(h not in file_props[v] for h in fdata.fieldnames):
				invalid_files.append(k)
	return invalid_files
