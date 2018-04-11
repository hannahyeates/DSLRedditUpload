import praw
from pprint import pprint



def getSubreddit(subName, reddit, subTable, userTable):
	sub = reddit.subreddit(subName)
	name = sub.display_name
	description = sub.description
	activeUsers = sub.subscribers
	rules = parseRules(sub.rules()['rules'])
	mods = []
	for moderator in sub.moderator():
		mods.append(moderator.name)
	return [buildSQL(name, description, rules, activeUsers, mods, subTable, userTable), mods]


def parseRules(rules):
	rawRules = ''
	for rule in rules:
		rawRules += rule['description']
	rawRules = rawRules.replace('"', '')
	rawRules = rawRules.replace('%', '')
	return rawRules

def buildSQL(sub, description, rules, activeUsers, mods, subTable, userTable):
	sql = {}
	sql['insertSub'] = 'INSERT INTO ' + subTable + ' VALUES("' + sub + '","' + description + '","' + rules + '",' + str(activeUsers) + ');'
	sql['insertModsInUsers'] = 'INSERT INTO ' + userTable + ' ( username ) VALUES'
	for mod in mods:
		sql['insertModsInUsers'] += '("'+ mod + '"),'
	sql['insertModsInUsers'] = sql['insertModsInUsers'][:-1]+ ';'
	return sql