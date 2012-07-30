import psycopg2
from psycopg2 import extras
import sys

class EasySQL:

	def __init__(self, h='localhost', db='vip', u='username', pw='password'):
		self.conn = psycopg2.connect(host=h,database=db,user=u,password=pw)
		self.cursor = self.conn.cursor(cursor_factory=extras.RealDictCursor)

	def simple_select(self, table, vals=None, conditions=None):
		query = "SELECT "
		if not vals:
			query += " * "
		else:
			query += ','.join(vals)
		query += " FROM " + table
		if conditions:
			query += " WHERE " + ' AND '.join(["{0} {1} {2}".format(k,conditions[k]['condition'],conditions[k]['compare_to']) for k in conditions])
		return query
	
	def select(self, tables, vals=None, conditions=None, result_count=None):

		if conditions:
			conditions = self.clean_conditions(conditions)
		if len(tables) == 1:#also need to check conditions for special values ie 'not in'
			query = self.simple_select(tables[0], vals, conditions)
		self.cursor.execute(query)

		if not result_count:
			return self.cursor.fetchall()
		elif result_count == 1:
			return self.cursor.fetchone()
		else:
			return self.cursor.fetchall()[0:result_count-1]

	def leftjoin(self, base_table, base_vals, base_conditions, join_table, join_vals, join_conditions, join_comparisons):
		query = "SELECT t1." + ",t1.".join(base_vals) + ",t2." + ",t2.".join(join_vals)
		query += " FROM " + base_table + " t1 "
		query += " JOIN " + join_table + " t2 ON ("
		query += ' AND '.join(["t1.{0} {1} t2.{0}".format(k,v) for k,v in join_comparisons.items()]) + ")"
		if len(base_conditions) > 0 and len(join_conditions) > 0:
			query += ' WHERE ' + ' AND '.join(["t1.{0} {1} '{2}'".format(k,base_conditions[k]['condition'],base_conditions[k]['compare_to']) for k in base_conditions])
			query += ' AND ' + ' AND '.join(["t2.{0} {1} '{2}'".format(k,join_conditions[k]['condition'],join_conditions[k]['compare_to']) for k in join_conditions])
		elif len(base_conditions) > 0:
			query += ' WHERE ' + ' AND '.join(["t1.{0} {1} '{2}'".format(k,base_conditions[k]['condition'],base_conditions[k]['compare_to']) for k in base_conditions])
		elif len(join_conditions) > 0:
			query += ' WHERE ' + ' AND '.join(["t2.{0} {1} '{2}'".format(k,join_conditions[k]['condition'],join_conditions[k]['compare_to']) for k in join_conditions])
		self.cursor.execute(query)
		return self.cursor.fetchall()

	def row_insert(self, table, vals):
		query = "INSERT INTO " + table + "("
		query += ','.join(vals.keys()) + ")"
		query += " VALUES ('" + "','".join(vals.values()) + "')"
		return query

	def insert(self, table, vals):
		for v in vals:
			query = self.row_insert(table, v)
			self.cursor.execute(query)
			self.conn.commit()
	
	def copy_upload(self, table, vals ,file_name):
		query = "COPY {0}({1}) FROM '{2}' WITH CSV HEADER".format(table, ",".join(vals), file_name)
		self.cursor.copy_expert(query, sys.stdin)
		self.conn.commit()

	def update(self, table, set_vals, conditions=None):
		query = "UPDATE " + table
		query += " SET " + ','.join(["{0} = '{1}'".format(k,v) for (k,v) in set_vals.items()])
		if conditions:
			conditions = self.clean_conditions(conditions)
			query += " WHERE " + ' AND '.join(["{0} {1} '{2}'".format(k,conditions[k]['condition'],conditions[k]['compare_to']) for k in conditions])
		self.cursor.execute(query)

	#Right now this will use truncate if no conditions are supplied, since
	#it will do more work cleaning up the table. If this gets too slow,
	#this should be changed to just use "delete from" for the table
	def delete(self, table, conditions=None):
		if conditions:
			conditions = self.clean_conditions(conditions)
			query = "DELETE FROM " + table + " WHERE " + ' AND '.join(["{0} {1} '{2}'".format(k,conditions[k]['condition'],conditions[k]['compare_to']) for k in conditions])
		else:
			query = "TRUNCATE " + table
		self.cursor.execute(query)
		self.conn.commit()

	def custom_query(self, query):
		self.cursor.execute(query)
		self.conn.commit()
		if query.lower().startswith("select"):
			return self.cursor.fetchall()

	def clean_conditions(self, conditions):
		temp_conditions = {}
		for k, v in conditions.items():
			if not isinstance(v, dict):
				temp_conditions[k] = {'compare_to':v, 'condition':'='}
			else:
				temp_conditions[k] = v
		return temp_conditions

if __name__ == '__main__':
	es = EasySQL()
	es.leftjoin('precinct',['feed_id AS "id"'],{'election_id':{'condition':'=','compare_to':'1000'},'vip_id':{'condition':'=','compare_to':'37'}},'precinct',['feed_id AS "duplicate_id"'],{},{'vip_id':'=','election_id':'=','feed_id':'!=','name':'='})
