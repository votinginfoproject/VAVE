import csv
import MySQLdb as mdb
import time

connection = mdb.connect('localhost', 'vip', 'username', 'password')
cursor = connection.cursor(mdb.cursors.DictCursor)
cursor.execute("DELETE FROM precinct")
cursor.execute("DELETE FROM polling_location")
cursor.execute("DELETE FROM simpleAddressType")
cursor.execute("DELETE FROM detailAddressType")
cursor.execute("DELETE FROM street_segment")

def get_precincts(data):
	precinct_info = {}
	for row in data:
		county = row["county"]
		name = int(float(row["precinct_name"]))
		if name > 100000:
			name = name/100
		if county not in precinct_info:
			precinct_info[county] = []
		precinct_info[county].append(name)
	for county in precinct_info:
		precinct_info[county] = list(set(precinct_info[county]))
	return precinct_info

def get_locality_ids():
	county_info = {}
	cursor.execute("SELECT * FROM locality")
	rows = cursor.fetchall()
	for r in rows:
		county_info[r["name"]] = r["id"]
	return county_info

def create_simple_address(row):
	address = row["Address"].split(",")
	address_insert = "INSERT INTO simpleAddressType (location_name, state"
	address_values = ") VALUES ('" + row["Location"].replace("'","") + "', 'NV'"
	if len(address) > 0:
		address_insert += ", line1"
		address_values += ", '" + address[0] + "'"
		if len(address) > 1:
			address_insert += ", city"
			address_values += ", '" + address[1] + "'"
			if len(address) > 2:
				address_insert += ", zip"
				address_values += ", '" + address[2].replace("NV", "").strip() + "'"

	cursor.execute(address_insert + address_values + ")")
	return cursor.lastrowid


		
voter_data = csv.DictReader(open("voter_file_clean.txt"))

precinct_codes = get_precincts(voter_data)

localities = get_locality_ids()

used_precincts = {}

precinct_info = {}

caucus_data = csv.DictReader(open("nv_precincts.csv"))

for row in caucus_data:
	county = row["County"]
	precinct_list = row["Precinct"]
	
	if len(row["County"]) <= 0 or len(row["Precinct"]) <= 0:
		#print row
		continue

	address_id = create_simple_address(row)
	
	if precinct_list == "All":

		used_precincts[county] = {} 
		locality_id = localities[county]

		for name in precinct_codes[county]:
			if len(str(locality_id)) < 4:
				new_id = int(str(locality_id) + str(name))
			else:
				new_id = int(str(int(locality_id)*2) + str(name))
			precinct_id = new_id + 200000000
			polling_location_id = new_id + 600000000
			cursor.execute("INSERT INTO precinct (id, element_id, locality_id, polling_location_id, name) VALUES('" + str(precinct_id) + "','" + str(precinct_id) + "','" + str(locality_id) + "','" + str(polling_location_id) + "','" + str(name) + "')")
			cursor.execute("INSERT INTO polling_location (id, element_id, address_id, polling_hours) VALUES('" + str(polling_location_id) + "','" + str(polling_location_id) + "','" + str(address_id) + "','" + row["Time"] + "')")
			used_precincts[county][int(name)] = precinct_id
	else:
		if county not in used_precincts:
			used_precincts[county] = {} 
		locality_id = localities[county]
		precinct_list = precinct_list.split(",")
		for precinct in precinct_list:
			precinct = precinct.strip()
			if len(str(precinct)) > 0:
				if len(str(locality_id)) < 4:
					new_id = int(str(locality_id) + str(precinct))
				else:
					new_id = int(str(int(locality_id)*2) + str(precinct))
				precinct_id = new_id + 200000000
				polling_location_id = new_id + 600000000
				cursor.execute("INSERT INTO precinct (id, element_id, locality_id, polling_location_id, name) VALUES('" + str(precinct_id) + "','" + str(precinct_id) + "','" + str(locality_id) + "','" + str(polling_location_id) + "','" + str(precinct) + "')")
				cursor.execute("INSERT INTO polling_location (id, element_id, address_id) VALUES('" + str(polling_location_id) + "','" + str(polling_location_id) + "','" + str(address_id) + "')")
				used_precincts[county][int(precinct)] = precinct_id


voter_data = csv.DictReader(open("voter_file_clean.txt"))
for row in voter_data:
	
	precinct_name = int(float(row["precinct_name"]))
	if precinct_name > 100000:
		precinct_name = precinct_name / 100
	if row["county"] in used_precincts and int(precinct_name) in used_precincts[row["county"]]:
		precinct_name = used_precincts[row["county"]][precinct_name]
	else:
		continue

	street_id = 800000000 + int(row["id"])

	address_insert = "INSERT INTO detailAddressType (street_direction, street_name, street_suffix, city, state, zip) VALUES('" + row["street_direction"] + "','" + row["street_name"].replace("'", "") + "','" + row["street_suffix"] + "','" + row["city"] + "','NV','" + row["zip"] + "')"
	cursor.execute(address_insert)
	cursor.execute("INSERT INTO street_segment (id, element_id, start_house_number, end_house_number, odd_even_both, start_apartment_number, end_apartment_number, non_house_address_id, precinct_id) VALUES ('" + str(street_id) + "','" + str(street_id) + "','" + str(row["street_number"]) + "','" + str(row["street_number"]) + "','both','" + str(row["apartment_number"]) + "','" + str(row["apartment_number"]) + "','" + str(cursor.lastrowid) + "','" + str(precinct_name) + "')")
