from lxml import etree
import MySQLdb as mdb

simpleAddressTypes = ["address", "physical_address", "mailing_address", "filed_mailing_address"]
detailAddressTypes = ["non_house_address"]
ELEMENT_LIST = ["source", "election", "state", "locality", "precinct", "street_segment", "polling_location"]
fname = 'TEST_VIPFeed-32-2011-12-13.xml'

vip_id = 32

xmlparser = etree.XMLParser()
connection = mdb.connect('localhost', 'vip', 'gamet1me', 'vip')
cursor = connection.cursor()

data = etree.parse(open(fname), xmlparser)
root = data.getroot()
elements = root.getchildren()
for element in elements:
	if element.tag in ELEMENT_LIST:
		sub_elems = element.getchildren()
		insert_str = 'INSERT INTO ' + element.tag + ' (id, element_id'
		val_str = ') VALUES (' + str(str(vip_id) + str(element.get('id'))) + ', ' + str(element.get('id'))
		for elem in sub_elems:
			if elem.tag in simpleAddressTypes or elem.tag in detailAddressTypes:
				add_elems = elem.getchildren()
				insert_str += ', ' + elem.tag + '_id'
				if elem.tag in simpleAddressTypes:
					add_insert_str = 'INSERT INTO simpleAddressType ('
				elif elem.tag in detailAddressTypes:
					add_insert_str = 'INSERT INTO detailAddressType ('
				add_val_str = ') VALUES ('
				for add_elem in add_elems:
					add_insert_str += add_elem.tag + ','
					if add_elem.text is None:
						add_val_str += '\"\",'
					else:
						add_val_str += '\"' + add_elem.text.replace('"', "'") + '\",'
				add_insert_str = add_insert_str[:add_insert_str.rfind(',')] + add_val_str[:add_val_str.rfind(',')] + ')'	
				cursor.execute(add_insert_str)
				val_str += ', \"' + str(cursor.lastrowid) + '\"'
			else:
				insert_str += ', ' + elem.tag
				if elem.text is None:
					val_str += ', \"\"'
				else:
					val_str += ', \"' + elem.text.replace('"', "'") + '\"'
		insert_str += val_str + ')'
		cursor.execute(insert_str)

connection.commit()
