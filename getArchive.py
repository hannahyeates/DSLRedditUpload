"""
 This file was used to get all of the old data. It is pretty hackey as of right now, but I will 
 update if we decide that it is worth doing to get specific data ranges.


Created on Thu Sep 14 08:51:48 2017
@author: wulff
"""
from praw.models import MoreComments

import praw
import datetime
from time import mktime
import logging
import mods
import pymysql as sql
from utils import remove_non_ascii_1
from getData import insertComment, getSubComments

# This is basic logging code provided by PRAW.  I didn't really alter anything
# here, so I don't have too much to comment on
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger = logging.getLogger('prawcore')
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Here's the initialization of the 'Reddit' object.  Client ID and secret
# are given by the authorization on the reddit side.  I'll link to some stuff
# on the github readme.  The user agent is a string that gives reddit basic
# information on what you're doing.

#INSERT YOUR INFO HERE
reddit = praw.Reddit(client_id='',
                     client_secret='',
                     user_agent='')

# This is just printing out whether you're in read only mode or not
print("Read Only?")
print(reddit.read_only)
print("\n")

#print(reddit.user.me())

# The way my code works is it takes a date range as a console input and prints
# out all the search results as a basically formatted HTML page
# Since I've been the only one using it thus far, it's a bit messy

# This initializes the subreddit you'd like to search through.  I haven't tried
# it, but I believe that 'all' works as a valid sub if you want to search
# through all of reddit
sub = reddit.subreddit("uwaterloo")

# Here's where the Python console takes inputs for date and converts them into
# int data types
year_s = int(input("Enter year start"))
month_s = int(input("Enter Month start"))
day_s = int(input("Enter Day start"))

year_e = int(input("Enter year end"))
month_e = int(input("Enter Month end"))
day_e = int(input("Enter Day end"))

# This converts the date values to unix-accepted values
start = datetime.date(year_s,month_s,day_s)
start = int(mktime(start.timetuple()))
print(start)

end = datetime.date(year_e,month_e,day_e)
end = int(mktime(end.timetuple()))
print(end)

# Here's the search string.  I'm interested in things involving rule changes on
# subs, so I put that in here.
searchstr = "'rule' timestamp:{}..{}".format(start,end)
tagsFound = []
conn = sql.connect(host='lg-research-1.uwaterloo.ca', user='hvgautreau', password='Hannah2017', db='Reddit', charset="utf8")
users = []
with conn.cursor() as cursor:
	cursor.execute('SELECT username FROM USER;' )
	results = cursor.fetchall()
	for row in results:
		users.append(row[0])
conn.commit()
subID = 1
for submission in sub.search(searchstr,sort='relevance',syntax='cloudsearch',limit=None):	
	for top_level_comment in submission.comments:
		if isinstance(top_level_comment, MoreComments):
			continue
		if top_level_comment.author != None: 
			insertComment(top_level_comment, conn, 'USER', 'COMMENT', users)

			if not hasattr(top_level_comment, "replies"):
				replies = top_level_comment.comments()
			else:
				replies = top_level_comment.replies
				for child in replies:
					if isinstance(top_level_comment, MoreComments):
						continue
					if child.author != None:
						insertComment(child, conn, 'USER', 'COMMENT', users)
						getSubComments(child, 'COMMENT', 'USER', conn, users)











'''	if submission.link_flair_text != None:
		if submission.link_flair_text not in tagsFound:
					tagsFound.append(submission.link_flair_text)
tagsInDB = []
with conn.cursor() as cursor:
	cursor.execute("SELECT * FROM TAG")
	results = cursor.fetchall()
	for row in results:
		tagsInDB.append(row[0])
conn.commit()

tags = []
for tag in tagsFound:
	if tag not in tagsInDB:
		tags.append(tag)

if len(tags) >0:
	sql = 'INSERT INTO TAG ( tag_id, tag_name) VALUES '
	for tag in tags:
		sql += '(default, "'+ str(tag) + '"),'
	sql = sql[:-1]+ ';'
	with conn.cursor() as cursor:
		cursor.execute(sql)
	conn.commit()'''













    
