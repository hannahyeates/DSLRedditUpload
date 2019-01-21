'''
	Wrapper to run all code to pull the most recent two weeks of data from 
	any indicated subreddits in the "subs" array.
	this will run in about 15 minutes, and it is recommended to run it on a
	weekly basis to ensure that we do not miss anything.
'''

import praw
from pprint import pprint
import csv
from praw.models import MoreComments
from getSubreddit import getSubreddit
import utils
import pymysql as sql
import pymysql.cursors
from getThreads import getThreads
from getComments import getComments
from getMods import insertMods
import datetime
from updateMaster import addDataToMaster

# In case tables were made, it deletes any tables created in the same day, and creates a fresh copy.
# This returns a dictionary of table and the raw table name. To access individual tables, simply 
# write tables['kind'] where kind is one of subreddit, tag, user, thread, comment, moderates
tables = utils.freshDB()

#connection to reddit that gives read access, add your information before running
reddit = praw.Reddit(client_id='',
                     client_secret='',
                     user_agent='')

# server connection - change user and password to your own username and password before running
conn = sql.connect(host='', user='', password='', db='', charset="utf8")


# ATTN: List any subreddits you want to pull here.
subs = ['uwaterloo']

#Iterate through subreddits to get all data.
for subName in subs:
	subreddit = getSubreddit(subName, reddit, tables['subreddit'], tables['user'])
	subredditSQL = subreddit[0]
	modList = subreddit[1]
	with conn.cursor() as cursor:
		cursor.execute(subredditSQL['insertSub'])
		cursor.execute(subredditSQL['insertModsInUsers'])
	conn.commit()
	insertMods(modList, subName, tables['moderates'], tables['user'], conn)
	getThreads(subName, tables['user'], tables['thread'], tables['tag'], reddit, conn)
	getComments(subName, tables['comment'], tables['user'], tables['thread'], tables['comment'], reddit, conn)

# once all the data has been pulled, add any new data to the master tables in the database
addDataToMaster(conn, tables)

