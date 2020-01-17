import pymongo
from pymongo import MongoClient
import json
import tweepy
import sys

from bson.binary import Binary
import pickle


#OAUTH

CONSUMER_KEY = "AK6kAGDDS5ZvLgdTzaskknqOx"
CONSUMER_SECRET = "MrHUTxf2L5uz4CWxBFkWzolsPlmePdSCvkzgtwJA2GzapDoWdA"
OAUTH_TOKEN = "177046565-jzXSDynrnLvwzB236PLAqju8cYgVMyl4CGBwcSzu"  # fill your oauth
OATH_TOKEN_SECRET = "lL0CC7pIJFxdXjAgwK8YfMjRO2z9TmC6ZgnHnroTnYyO5"  # fill your oauth

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(OAUTH_TOKEN, OATH_TOKEN_SECRET)

api = tweepy.API(auth)


#connect mongodb database


client = MongoClient()
db = client.tweet_cmd1
tweet_collection = db.tweet_collection
db.tweet_collection.drop()
tweet_collection.create_index([("id", pymongo.ASCENDING)],default_language='english')



#tweet_collection.create_index([("id", pymongo.ASCENDING)],unique=True)  # make sure the collected tweets are unique
#tweet_collection.dropIndex("id_1")
#tweet_collection.create_index([("id", pymongo.ASCENDING)], unique=True)  # make sure the collected tweets are unique
#tweet_collection.create_index([("id", pymongo.ASCENDING)],default_language='english')


#fetch data

header = ['timestamp', 'tweet_text', 'username', 'all_hashtags', 'followers_count']
dict_of_tweet={}

def search_for_hashtags(hashtag_phrase="#Shaho"):
    for tweet in tweepy.Cursor(api.search, q=hashtag_phrase + ' -filter:retweets',lang="en", tweet_mode='extended').items(100):
        json_obj = ([tweet.created_at, tweet.full_text.replace('\n', ' ').encode('utf-8'),
                    tweet.user.screen_name.encode('utf-8'), [e['text'] for e in tweet._json['entities']['hashtags']],
                    tweet.user.followers_count])
        j = 0
        for element in json_obj:
            dict_of_tweet[header[j]]=element
            j += 1

        #print(dict_of_tweet)
        for data in dict_of_tweet:
            thebytes = pickle.dumps(data)
            db.tweet_collection.insert_one({'bin-data': Binary(thebytes)})
                
        #db.tweet_collection.save(dict(dict_of_tweet))
        #tweet_collection.insert_many(dict_of_tweet)
        #tweet_collection.insert_one(obj)


#query collected data in MongoDB


tweet_cursor = tweet_collection.find()

#print(tweet_cursor.count())

user_cursor = tweet_collection.distinct("user.id")

#print(len(user_cursor))

for document in tweet_cursor:
    #client.update_one({'_id': doc['_id']}, doc, upsert=True)
    try:
        print('----')
        print (document)

        #print('name:', document["user"]["name"])
        #print('text:', document["text"])
    except:
        print("***error in encoding")

search_for_hashtags()

