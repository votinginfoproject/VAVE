from easysql import EasySQL

DEFAULT_ELECT_ID = 1000

class VaveDB(EasySQL):

	def __init__(self, h, db, u, pw, elect_id=None, v_id=None):
		EasySQL.__init__(self, h, db, u, pw)
		self.election_id = elect_id
		self.feed_id = v_id
	
	def get_election_id(self, feed_details):
		table_list = ['elections']
		result = self.select(table_list,['election_id'],feed_details,1)
		if not result:
			last_id = self.select(table_list,'GREATEST(election_id)',None,1)
			if not last_id:
				new_id = DEFAULT_ELECT_ID
			else:
				new_id = int(last_id["greatest"]) + 1
			feed_details["election_id"] = new_id
			self.insert(table_list[0],feed_details)
		else:
			return result["election_id"]
		return feed_details["election_id"]

	
