class MysqlUtils:
	def execute(conn, query, print_query=False, retry=True):
		with closing(conn.cursor()) as cursor:
			if print_query:
				print(query)
			try:
				cursor.execute(query)
				conn.commit()
			except:
				print(traceback.format_exc())
				print("Retrying connction")
				conn = Utils.mysqlConnection()
				cursor.execute(query)
				conn.commit()

