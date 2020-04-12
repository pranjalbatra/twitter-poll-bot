import json
import tweepy
import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import re
import urllib
import urllib2
import pymysql.cursors

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='twitter_integ',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

consumer_key=""
consumer_secret=""
access_token=""
access_token_secret=""



auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Tweet info
api = tweepy.API(auth)
# results = api.search(q="")
# for result in results:
#     print (result.text)
#     print "next tweet \n"


# For live Analysis 

trackables = ["#pb_create_a_poll_please","#myvote","#pb_i_seek_answers"]

#get all hashtags to listen on
# with connection.cursor() as cursor:
# 	sql = "SELECT `hashtag` FROM `poll_data`"
# 	cursor.execute(sql);
# 	res = cursor.fetchall()
# 	for r in res:
# 		trackables.append('#'+r['hashtag'])


def sendTweet(tweet):
  	status = api.update_status(status=tweet)

def validateData(post,username):
	s = post.split(" ")
	hashTag = s[0]
	hashTag = hashTag.strip()
	hashTag = hashTag.lower()
	if hashTag not in trackables:
		return 'Invalid Command';
	if hashTag == "#pb_create_a_poll_please":
		s[0] = " ";
		sel = " "
		s = sel.join(s)
		s = s.strip()
		jsonData = s
		try:
			data = json.loads(jsonData)
		except ValueError, e:
			return "Invalid Command";
		if data.has_key('title'):
			if data['title'] == '':
				return 'Title cannot be empty';
		else:
			return 'Title not found';		
		if data.has_key('hashtag'):
			if data['hashtag'] == '': 
				return 'Hashtag cannot be empty';
			#check from db if this hashtag is taken	
			data['hashtag'] = data['hashtag'].lower()
			with connection.cursor() as cursor:
				sql = "SELECT `id` FROM `poll_data` WHERE `hashtag` = %s"
				res = cursor.execute(sql,data['hashtag'])
				if res > 0:
					return 'This hashtag is taken. Choose a different one';
		else:
			return 'Hashtag not found';	
		if data.has_key('options'):
			if len(data['options']) < 2:
				return 'More than two options are required';
			for o in data['options']:
				if o == '': 
					return 'Option title cannot be empty';
		else:
			return 'Options not found';
		#Proper json found, now store in db
		with connection.cursor() as cursor:
			sql = "INSERT INTO `poll_data` (`poll_json`, `hashtag`) VALUES (%s, %s)"
			cursor.execute(sql,(jsonData,'#'+data['hashtag']))
			connection.commit()
		return "Poll Created Successfully";	
	elif hashTag == "#myvote":
		ansHash = s[1];
		ansHash = ansHash.strip()
		#check if user has answered, do not submit again
		with connection.cursor() as cursor:
			sql = "SELECT `id` FROM `poll_userdata` WHERE `hashtag` = %s AND `username` = %s" 
			res = cursor.execute(sql,(ansHash,username))
			if res != 0:
				return 'Looks like you have already answered this';
		#check if a poll with this hashtag exists
		with connection.cursor() as cursor:
			sql = "SELECT `poll_json` FROM `poll_data` WHERE `hashtag` = %s"
			res = cursor.execute(sql,ansHash)
			if res != 1:
				return ansHash+': No poll found with this hashtag';
		jsonData = cursor.fetchone()
		jsonData = jsonData['poll_json']
		try:
			data = json.loads(jsonData)
		except ValueError, e:
			return "An Error Occoured";
		s[0] = " ";
		s[1] = " ";
		sel = " "
		s = sel.join(s)
		answer = s.strip()
		answer = answer.lower()
		ctr = 0;
		for o in data['options']:
			if o.lower() == answer: 
				ctr = ctr + 1
		if ctr == 0:
			return 'No such option found';
		else:
			with connection.cursor() as cursor:
				sql = "INSERT INTO `poll_userdata` (`username`,`hashtag`, `answer`,`answer_date`) VALUES (%s, %s,%s,%s)"
				ts = time.time()
				ts = str(ts)
				ts = ts.split(".")
				ts = ts[0]
				cursor.execute(sql,(username,ansHash,answer,ts))
				connection.commit()
		return "Your response has been saved";						
	elif hashTag == "#pb_i_seek_answers":
		ansHash = s[1];
		ansHash = ansHash.strip()
		#check if a poll with this hashtag exists
		with connection.cursor() as cursor:
			sql = "SELECT `poll_json` FROM `poll_data` WHERE `hashtag` = %s"
			res = cursor.execute(sql,ansHash)
			if res != 1:
				return ansHash+': No poll found with this hashtag';
		jsonData = cursor.fetchone()
		jsonData = jsonData['poll_json']
		try:
			data = json.loads(jsonData)
		except ValueError, e:
			return "An Error Occoured";
		total = 0;
		options = []
		for o in data['options']:
			obj = {}
			obj['option'] = o
			opt = o.lower()
			with connection.cursor() as cursor:
				sql = "SELECT `id` FROM `poll_userdata` WHERE `hashtag` = %s AND `answer` = %s"
				res = cursor.execute(sql,(ansHash,opt))
				total = total + res
				obj['votes'] = res
			options.append(obj)	
		if total == 0:
			total = 1;
		i = 0
		result = ''
		for o in options:
			options[i]['percentage'] = (o['votes']* 100)/total
			result = result + str(o['option']) + ' - ' + str(options[i]['percentage']) + '% (' + str(o['votes']) +' vote(s)) '
			i = i + 1	
		return result;		
	return 'success';		


# mystring = '#pb_create_a_poll_please {"title":"Who is better?","options":["Jay Z","Eminem","Dr. Dre"],"hashtag":"PollTester"}'
# mystring2 = '#myvote #polltester jay z'
# mystring3 = '#pb_i_seek_answers #polltester'
# msg = validateData(mystring3,'timF')
# print(msg)

class listener(StreamListener):
      def on_data(self, data):
          # print(data)
          decoded = json.loads(data)
          msg = validateData(decoded['text'],decoded['user']['screen_name'])
          print(decoded['text'])
          user = '@' + decoded['user']['screen_name']
          msg = user + ' ' + msg
          sendTweet(msg)	
          return True
      def on_error(self, status):
          print(status)
ts = Stream(auth, listener())
ts.filter(track=trackables)




# public_tweets = api.home_timeline()
# for tweet in public_tweets:
# 	print tweet



