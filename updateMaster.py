'''
	This file dumps all of the new data into the master data set, then deletes teh temporary tables
'''


from pprint import pprint
import pymysql as sql
import pymysql.cursors

def addDataToMaster(conn, tables):

	subredditSQL ='INSERT INTO SUBREDDIT SELECT * FROM ' + tables['subreddit'] + ' WHERE sub_name NOT IN (SELECT sub_name FROM SUBREDDIT);'
	userSQL = 'INSERT INTO USER SELECT * FROM ' + tables['user'] + ' WHERE username NOT IN (SELECT username FROM USER);'
	modsSQL = 'INSERT INTO MODERATES SELECT NULL, m.sub_name, m.username FROM ' + tables['moderates'] + ' as m WHERE NOT EXISTS (SELECT * FROM MODERATES as x WHERE x.sub_name = m.sub_name AND m.username = x.username);'
	tagSQL = 'INSERT INTO TAG SELECT * FROM ' + tables['tag'] + ' WHERE tag_name NOT IN (SELECT tag_name FROM TAG);'
	threadSQL = 'INSERT INTO THREAD SELECT * FROM ' + tables['thread'] + ' WHERE thread_id NOT IN (SELECT thread_id FROM THREAD);'
	commentSQL = 'INSERT INTO COMMENT SELECT * FROM ' + tables['comment'] + ' WHERE comment_id NOT IN (SELECT comment_id FROM COMMENT);'

	with conn.cursor() as cursor:
		cursor.execute(subredditSQL)
		cursor.execute(userSQL)
		cursor.execute(modsSQL)
		cursor.execute(tagSQL)
		cursor.execute(threadSQL)
		cursor.execute(commentSQL)
		for table in tables:
			cursor.execute('DROP TABLE IF EXISTS  ' + tables[table] + ';')