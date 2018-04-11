import pymysql as sql
import pymysql.cursors


def insertMods(mods, sub, modTable, userTable, conn):
	sql = 'INSERT INTO ' + modTable + '( mod_id, sub_name, username ) VALUES '
	for mod in mods:
		sql += '(default, "'+ sub + '","'  +  mod +'" ),'
	sql = sql[:-1]+ ';'
	with conn.cursor() as cursor:
		cursor.execute(sql)
	conn.commit()




