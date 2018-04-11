import pymysql as sql
import pymysql.cursors
import datetime


def remove_non_ascii_1(text):
	return ''.join([i if ord(i) < 128 else ' ' for i in text])

def freshDB():  
	conn = sql.connect(host='lg-research-1.uwaterloo.ca', user='hvgautreau', password='Hannah2017', db='Reddit')
	now = datetime.datetime.now()
	nowDate = str(now.year) + str(now.month) + str(now.day)
	tables = {'moderates': 'MODERATES_' + nowDate,
			'comment':'COMMENT_' + nowDate,
			'thread':'THREAD_'+ nowDate, 
			'tag': 'TAG_'+nowDate, 
			'user': 'USER_'+nowDate,
			'subreddit':'SUBREDDIT_'+nowDate
			}

	mods = """CREATE TABLE MODERATES_""" + nowDate + """  (
		`mod_id` int(11) NOT NULL AUTO_INCREMENT,
  		`sub_name` varchar(45) NOT NULL,
  		`username` varchar(45) NOT NULL,    
		PRIMARY KEY (`mod_id`),       
		FOREIGN KEY (`sub_name`) REFERENCES """ + tables['subreddit']+ """ (`sub_name`),    
		FOREIGN KEY (`username`) REFERENCES """ + tables['user']+ """ (`username`));
		"""
  	
	comments = """
		CREATE TABLE COMMENT_""" + nowDate + """  (
  		`comment_id` varchar(16) NOT NULL,
  		`is_top_level` tinyint(4),
  		`body` text NOT NULL,
  		`create_dt` int(11) NOT NULL,
  		`score` int(11),
  		`thread_id` varchar(16) NOT NULL,
  		`username` varchar(45) NOT NULL,
  		`parent` varchar(16) DEFAULT NULL,
  		`sub_name` varchar(45) NOT NULL,
  		PRIMARY KEY (`comment_id`),
  		FOREIGN KEY (`username`) REFERENCES """ + tables['user']+ """ (`username`),
  		FOREIGN KEY (`thread_id`) REFERENCES """ + tables['thread']+ """ (`thread_id`)
		);
		"""

	thread = """
		CREATE TABLE THREAD_""" + nowDate + """  (
  		`thread_id` varchar(16) NOT NULL,
  		`title` text NOT NULL,
  		`content` text,
  		`create_dt` int(11) NOT NULL,
  		`score` int(11) DEFAULT NULL,
  		`sub_name` varchar(45) NOT NULL,
  		`tag_name` varchar(45) DEFAULT NULL,
  		`username` varchar(45) NOT NULL,
  		PRIMARY KEY (`thread_id`),
  		FOREIGN KEY (`sub_name`) REFERENCES """ + tables['subreddit']+ """(`sub_name`),
  		FOREIGN KEY (`tag_name`) REFERENCES """ + tables['tag']+ """(`tag_name`),
  		FOREIGN KEY (`username`) REFERENCES """ + tables['user']+ """(`username`)
		); 
		"""
	tag = """
		CREATE TABLE TAG_""" + nowDate + """  (
  		`tag_name` varchar(45) NOT NULL,
  		PRIMARY KEY (`tag_name`)
  		);
		"""

	user = """
		CREATE TABLE USER_""" + nowDate + """  (
  		`username` varchar(45) NOT NULL,
  		PRIMARY KEY (`username`)
  		);
  		"""
	subreddit = """
		CREATE TABLE SUBREDDIT_""" + nowDate + """ (
  		`sub_name` varchar(45) NOT NULL,
  		`description` text,
  		`rules` text,
  		`num_readers` int(11),
  		PRIMARY KEY (`sub_name`)
  		);
  		"""

	with conn.cursor() as cursor:
		for table in tables:
			cursor.execute('DROP TABLE IF EXISTS  ' + tables[table] + ';')
		cursor.execute(subreddit)
		cursor.execute(user)
		cursor.execute(tag)
		cursor.execute(thread)
		cursor.execute(mods)
		cursor.execute(comments)

	conn.commit()
	return tables