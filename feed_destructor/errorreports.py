import directorytools as dt
from shutil import copyfile
from csv import DictWriter

REPORT_DIRECTORY = "/tmp/reports/"

def report_setup(vip_id=None):
	dt.create_directory(REPORT_DIRECTORY)
	if not vip_id:
		dt.create_directory(REPORT_DIRECTORY+"unknown")
	else:
		dt.create_directory(REPORT_DIRECTORY+vip_id)
		dt.create_directory(REPORT_DIRECTORY+vip_id + "/archives")
		dt.clear_or_create(REPORT_DIRECTORY+vip_id + "/current")

def report_summary(vip_id, election_id, file_details, election_details):
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
	writer.write("Election Date: " + str(election_details["date"]) + "\n")
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

def e_count_summary(feed_details, element_counts):
	fname = "report_summary_" + feed_details["file_time_stamp"] + ".txt"

	directory = REPORT_DIRECTORY + str(feed_details["vip_id"]) + "/"
	with open(directory + "current/" + fname, "a") as w:
		writer.write("----------------------\nElement Counts\n----------------------\n\n")
		for elem in element_counts:
			writer.write(elem + "\n")
			writer.write("\t- original:" + str(element_counts[elem]['original'])+"\n")
			writer.write("\t- processed:" + str(element_counts[elem]['processed']) + "\n")
	copyfile(directory + "current/" + fname, directory + "archives/" + fname)	
