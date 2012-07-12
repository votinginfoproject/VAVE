import psycopg2
from psycopg2 import extras

class EasySQL:

	def __init__(self, h='localhost', db='vip', u='username', pw='password'):
		self.conn = psycopg2.connect(host=h,database=db,user=u,password=pw)
		self.cursor = self.conn.cursor(cursor_factor=extras.RealDictCursor)

	def basic_select(self, table, *vals=None, conditions=None):
		query = "SELECT "
		if not vals:
			query += " * "
		else:
			query += ','.join(vals)
		query += " FROM " + table
		if conditions:
			query += ' AND '.join(["{0} {1} '{2}'".format(k,conditions[k]['condition'],conditions[k]['compare_to']) for k in conditions])
		return query
	
	def select(self, *tables, *vals=None, conditions=None, result_count=None):

		if conditions:
			conditions = self.clean_conditions(conditions)
		if len(tables) == 1:
			query = self.basic_select(tables[0], vals, conditions)

		self.cursor.execute(query)

		if not result_count:
			return self.cursor.fetchall()
		elif result_count == 1:
			return self.cursor.fetchone()
		else:
			return self.cursor.fetchall()[0:result_count-1]

	def clean_conditions(self, conditions):
		temp_conditions = {}
		for k, v in conditions.items():
			if "condition" in v:
				temp_conditions[k] = v
			else:
				temp_conditions[k] = {'compare_to':v, 'condition':'='}
		return temp_conditions
			
	def get_election_id(self, feed_details):
		query = "SELECT election_id FROM elections WHERE " + ' AND '.join(["{0} = '{1}'".format(key, value) for (key, value) in feed_details.items()])
		self.cursor.execute(query)
		result = self.cursor.fetchone()
		if not result:
			self.cursor.execute("SELECT GREATEST(election_id) FROM elections")
			last_id = self.cursor.fetchone()
			if not last_id:

