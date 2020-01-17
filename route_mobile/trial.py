import pymongo
from pymongo import MongoClient
import json
import tweepy
import sys

from bson.binary import Binary
import pickle

'''
OAUTH
'''
CONSUMER_KEY = "AK6kAGDDS5ZvLgdTzaskknqOx"
CONSUMER_SECRET = "MrHUTxf2L5uz4CWxBFkWzolsPlmePdSCvkzgtwJA2GzapDoWdA"
OAUTH_TOKEN = "177046565-jzXSDynrnLvwzB236PLAqju8cYgVMyl4CGBwcSzu"  # fill your oauth
OATH_TOKEN_SECRET = "lL0CC7pIJFxdXjAgwK8YfMjRO2z9TmC6ZgnHnroTnYyO5"  # fill your oauth

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(OAUTH_TOKEN, OATH_TOKEN_SECRET)

api = tweepy.API(auth)

'''
connect mongodb database
'''

client = MongoClient()
db = client.tweet_scrap
tweet_collection = db.tweet_collection
db.tweet_collection.drop()
#tweet_collection.create_index([("id", pymongo.ASCENDING)], unique=True)  # make sure the collected tweets are unique

tweet_collection.create_index([("id", pymongo.ASCENDING)],default_language='english')
#tweet_collection.create_index([("id", pymongo.ASCENDING)], unique=True)  # make sure the collected tweets are unique
#tweet_collection.Term.dropIndex("id_1")

'''
define query in Stream API
'''

track = ['election']

locations = [-84.56, 33.62, -84.20, 33.91]

'''
fetch data
'''
header = ['timestamp', 'tweet_text', 'username', 'all_hashtags', 'followers_count']
dict_of_tweet={}

def search_for_hashtags(hashtag_phrase="Ashish Gore"):
    for tweet in tweepy.Cursor(api.search, q=hashtag_phrase + ' -filter:retweets',lang="en", tweet_mode='extended').items(100):
        json = ([tweet.created_at, tweet.full_text.replace('\n', ' ').encode('utf-8'),
                    tweet.user.screen_name.encode('utf-8'), [e['text'] for e in tweet._json['entities']['hashtags']],
                    tweet.user.followers_count])
        j = 0
        for element in json:
            dict_of_tweet[header[j]]=element
            j += 1

        db.tweet_collection.save(dict_of_tweet)

        #db.tweet_collection.save((dict(dict_of_tweet)))

        #tweet_collection.insert_many(dict_of_tweet)
        #tweet_collection.insert_one(obj)

    '''
            for num in range(0, len(tweets)):
                row = {}
                for each in tweets:
                    j = 0
                    for value in each:
                        row[header[j]] = value
                        row.update({header[j]: value})
                        list_of_tweets.append(row)
                        j += 1
                        
    '''
search_for_hashtags()



#query collected data in MongoDB

tweet_cursor = tweet_collection.find()
print(tweet_cursor)
print(type(tweet_cursor))

#print(tweet_cursor.count())

user_cursor = tweet_collection.distinct("user.id")
print(user_cursor)

#print(len(user_cursor))

for document in tweet_cursor:
    try:
        print('----')
        #         print (document)

        print('name:', document["user"]["name"])
        print('text:', document["text"])
    except:
        print("***error in encoding")
