'''
	Gets information related to the threads in the indicated subreddit
	Starts by getting everything in 'new', then gets anything from 'hot'
	that was not also in 'new'
'''

import praw
from praw.models import MoreComments
from pprint import pprint
import pymysql as sql
import pymysql.cursors
from utils import remove_non_ascii_1

# Function to insert all new and hot threads into the database
def getThreads(subName, userTable, threadTable, tagTable, reddit, conn):
	sub = reddit.subreddit(subName)
	# get all of the users that are currently in the database to avoid duplicates
	users = []
	with conn.cursor() as cursor:
		cursor.execute('SELECT username FROM ' + userTable + ';' )
		results = cursor.fetchall()
		for row in results:
			users.append(row[0])
	conn.commit()
	
	ids = []
	tags = []
	# Insert all new threads that the API will collect
	for submission in sub.new(limit=None):
		if submission.author != None:
			# record the ID to avoid duplicates
			ids.append(submission.id)
			insertThread(subName, userTable, threadTable, tagTable, reddit, conn, submission, users, tags)
	# Insert all hot threads that the API will collect
	for submission in sub.hot(limit=None):
		if submission.author != None and submission.id not in ids: 
			insertThread(subName, userTable, threadTable, tagTable, reddit, conn, submission, users, tags)

				
# Where the actual insertion of the data happens. We collect the ID, title, content, the utc time of creation, 
# score (upvotes - downvotes), subreddit name, tags, and the username of the author of the post
def insertThread(subName, userTable, threadTable, tagTable, reddit, conn, submission, users, tags):
	
	# first, add the author to the users table if they are not already there
	if submission.author.name not in users: 
		with conn.cursor() as cursor:
			users.append(submission.author.name)
			cursor.execute('INSERT INTO ' + userTable + ' (username) VALUES ("' + submission.author.name + '");' )
		conn.commit()
	
	# remove quotes to ensure the query can run
	text = submission.selftext.replace('"', '')
	title = submission.title.replace('"', '')
	if submission.link_flair_text is not None and submission.link_flair_text not in tags:
		sql = 'INSERT INTO ' + tagTable + ' (tag_name) VALUES ("' + submission.link_flair_text + '");'
		tags.append(submission.link_flair_text)
		with conn.cursor() as cursor:
			cursor.execute(sql)
		conn.commit()
	sql = ("""
		INSERT INTO """ + threadTable  + """ (thread_id, title, content, create_dt, score, sub_name, tag_name, username) 
		VALUES (\"""" + submission.id + """\", \"""" + remove_non_ascii_1(title) + """\",\"""" + remove_non_ascii_1(text) + """\", """
		+ str(submission.created_utc)  + ""","""
		+ str(submission.ups)  + """,\"""" 
		+ subName 
		+ """\",\""""  + str(submission.link_flair_text) +  """\" ,\"""" + submission.author.name + """\");""")

	sql = sql.replace("\"None\"", "NULL")
	with conn.cursor() as cursor:
		cursor.execute(sql)
	conn.commit()
