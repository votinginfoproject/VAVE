import directorytools as dt
from shutil import copyfile
from csv import DictWriter

REPORT_DIRECTORY = "/tmp/reports/"
ERROR_CODES = {"missing_required":"Missing required field",
		"non_integer":"Invalid integer given",
		"invalid_string":"String values must be valid ASCII and XML types",
		"non_iso_date":"Dates must be valid ISO dates",
		"non_iso_datetime":"DateTimes must be valid ISO dateTimes",
		"invalid_yesnoenum":"Must be either 'yes' or 'no'",
		"invalid_oebenum":"Must be 'odd', 'even', or 'both'",
		"invalid_locality":"Locality type must be valid VIP locality type",
		"invalid_street_dir":"Street directions must be valid"}
WARNING_CODES = {"invalid_zip":"Invalid Zip Code",
		"invalid_email":"Invalid E-mail",
		"invalid_url":"Invalid URL",
		"invalid_phone":"Invalid phone number",
		"invalid_state_abbrev":"State must be a valid 2-letter abbreviation",
		"invalid_hour_range":"Hour values must include a range",
		"invalid_end_house":"Ending house number must be greater than zero",
		"invalid_end_apartment":"Ending apartment number must be greater than zero"}

def report_setup(vip_id=None):
	dt.create_directory(REPORT_DIRECTORY)
	if not vip_id:
		dt.create_directory(REPORT_DIRECTORY+"unknown")
	else:
		dt.create_directory(REPORT_DIRECTORY+vip_id)
		dt.create_directory(REPORT_DIRECTORY+vip_id + "/archives")
		dt.clear_or_create(REPORT_DIRECTORY+vip_id + "/current")

def report_summary(vip_id, election_id, file_details, election_details, element_counts=None):
	dt.create_directory(REPORT_DIRECTORY)
	fname = "report_summary_" + file_details["file_timestamp"] + ".txt"

	if not vip_id:
		directory = REPORT_DIRECTORY + "unknown"
		dt.create_directory(directory)
		with open(fname, "w") as w:
			summary_header(file_details, w)
			file_summary(file_details, w)
			w.write("Missing source information, could not process feed")
	else:
		directory = REPORT_DIRECTORY + str(vip_id) + "/"
		dt.create_directory(directory)
		dt.create_directory(directory + "archives/")
		dt.clear_or_create(directory + "current/")
		with open(directory + "current/" + fname, "w") as w:
			summary_header(file_details, w)
			source_summary(vip_id, file_details, w)
			if not election_id:
				file_summary(file_details, w)
				w.write("Missing election information, could not process feed")
			else:
				election_summary(election_details, w)
				file_summary(file_details, w)
			if element_counts:
				w.write("----------------------\nElement Counts\n----------------------\n\n")
				for k, v in element_counts.iteritems():
					w.write(k + ":" + v + "\n")
		copyfile(directory + "current/" + fname, directory + "archives/" + fname)

def summary_header(file_details, writer):
	writer.write("File Processed: " + file_details["file"] + "\n")
	writer.write("Time Processed: " + file_details["process_time"] + "\n\n")

def file_summary(file_details, writer):
	writer.write("----------------------\nFile Report\n----------------------\n\n")
	if "invalid_sections" in file_details:
		writer.write("Invalid Sections: " + str(file_details["invalid_sections"]) + "\n")
	if "invalid_files" in file_details:
		writer.write("Invalid Files: " + str(file_details["invalid_files"]) + "\n")
	if "valid_files" in file_details:
		writer.write("Valid Files: " + str(file_details["valid_files"]) + "\n")
	writer.write("\n")

def source_summary(vip_id, feed_details, writer):
	writer.write("----------------------\nSource Data\n----------------------\n\n")
#	writer.write("Name: " + feed_details["name"] + "\n")
	writer.write("Vip ID: " + str(vip_id) + "\n")
#	writer.write("Datetime: " + str(feed_details["datetime"]) + "\n\n")

def election_summary(election_details, writer):
	writer.write("----------------------\nElection Data\n----------------------\n\n")
	writer.write("Election ID: " + str(election_details["election_id"]) + "\n")
	writer.write("Election Date: " + str(election_details["election_date"]) + "\n")
	writer.write("Election Type: " + str(election_details["election_type"]) + "\n\n")

def feed_issues(vip_id, file_timestamp, problem_data, issue_type):
	fname = "feed_" + issue_type + "s_" + file_timestamp + ".txt"
	cur_dir = REPORT_DIRECTORY + str(vip_id) + "/current/"
	arc_dir = REPORT_DIRECTORY + str(vip_id) + "/archives/"
	with open(cur_dir + fname, "a") as writer:
		out = DictWriter(writer, fieldnames=problem_data[0].keys())
		out.writeheader()
		for row in problem_data:
			out.writerow(row)
	copyfile(cur_dir + fname, arc_dir + fname)
