import csv
from ConfigParser import ConfigParser

def invalid_config_sections(directory, config_file, section_props):
	
	config = ConfigParser()
	config.read(config_file)
	sections = config.sections()
	invalid_sections = []
	for s in sections:
		if s not in section_props:
			invalid_sections.append(s)
		else:
			header = config.get(s, "header").split(",")
			if any(h not in section_props[s] for h in header):
				invalid_sections.append(s)
				continue
			with open(directory + config.get(s, "file_name") as f:
				fdata = csv.reader(f)
				try:
					if len(fdata.next()) != len(fieldnames):
						invalid_sections.append(s)
				except:
					invalid_sections.append(s)
	return invalid_sections
