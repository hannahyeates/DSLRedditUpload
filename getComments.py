'''
	Gets all comments in the indicated subreddit
	Starts by getting all comments on threads in 'new', 
	then gets all comments on threads in 'hot' that were not also in 'new'
'''

import praw
from praw.models import MoreComments
from pprint import pprint
import pymysql as sql
import pymysql.cursors
from utils import remove_non_ascii_1
from getThreads import insertThread


# Function to insert all comments into the database
def getComments(subName, commentTable, userTable, threadTable, tagTable, reddit, conn):
	sub = reddit.subreddit(subName)
	
	# Get all existing users to avoid duplicates
	users = []
	with conn.cursor() as cursor:
		cursor.execute('SELECT username FROM ' + userTable + ';' )
		results = cursor.fetchall()
		for row in results:
			users.append(row[0])
	conn.commit()
	
	# Get all threads that are currently in the database
	threads = []
	with conn.cursor() as cursor:
		cursor.execute('SELECT thread_id FROM ' + threadTable + ';' )
		results = cursor.fetchall()
		for row in results:
			threads.append(row[0])
	conn.commit()
	sub = reddit.subreddit(subName)
		
	for thread in threads:
		submission = reddit.submission(id=thread)

		# make sure we do not miss any threads that could have been added at the last minute
		if submission.id not in threads:
			insertThread(subName, userTable, threadTable, tagTable, reddit, conn, submission, users)
			threads.append(submission.id)
		if submission.author != None:
			for comment in submission.comments:
					if isinstance(comment, MoreComments):
						continue
					insertComment(comment, conn, userTable, commentTable, users, subName)

				
# Where the actual insertion of the data happens. We collect the ID, text,the utc time of creation, 
# score (upvotes - downvotes), the comment author, the parent id (null if it is a top level comment), 
# and the subreddit to which it belongs
def insertComment(comment, conn, userTable, commentTable, users, subName):
	
	if comment.author != None: 
		# first, add the author to the users table if they are not already there
		if comment.author.name not in users: 
			with conn.cursor() as cursor:
				users.append(comment.author.name)
				cursor.execute('INSERT INTO ' + userTable + ' (username) VALUES ("' + comment.author.name + '");' )
			conn.commit()
			is_top_level = 0
			# threads are indicated with a t3 prefix. If a thread is a comment's parent, we know it is a top level comment
			if comment.parent_id[:2] == 't3':
				is_top_level = 1
			body = comment.body.replace('"', '').replace('\\', ' ')
			sql = ("""
				INSERT INTO """ + commentTable  + """ (comment_id, is_top_level, body, create_dt, score, thread_id, username, parent, sub_name) 
				VALUES (\"""" + comment.id + """\", """ + str(is_top_level) + """,\"""" + remove_non_ascii_1(body) +  """\","""
				+ str(comment.created_utc)  + ""","""
				+ str(comment.ups) + """,\""""
				+ comment.link_id[-6:].replace('_', '')  + """\",\"""" + comment.author.name)
			
			# ensure we only have a parent comment for not top level comments
			if is_top_level == 1:
				sql += '",NULL,"' + subName + '");'
			else:
				sql +=  """\",\"""" + comment.parent_id[-7:].replace('_', '') + """\",\"""" + subName + """\");"""

			sql = sql.replace("\"None\"", "NULL")	
			
			print(sql)
			with conn.cursor() as cursor:
				cursor.execute(sql)
			conn.commit()
			if not hasattr(comment, "replies"):
					replies = comment.comments()
			else:
				replies = comment.replies
				for child in replies:
					if isinstance(child, MoreComments):
						continue
					if child.author != None:
						insertComment(child, conn, userTable, commentTable, users, subName)
						getSubComments(child, commentTable, userTable, conn, users, subName)





#function to access all of the child comments from a parent comment.
# function is called recursively until leaf comments are reached
def getSubComments(comment, commentTable, userTable, conn, users, subName):
	if not hasattr(comment, "replies"):
		replies = comment.comments()
	else:
		replies = comment.replies
		for child in replies:
			# ensures it doesn't crash after ~100 comments
			if isinstance(child, MoreComments):
				continue
			if child != None:
				if child.author != None:
					insertComment(child, conn, userTable, commentTable, users, subName)
					getSubComments(child, commentTable, userTable, conn, users, subName)


