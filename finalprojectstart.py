# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 22:32:36 2020

@author: Student
"""


import tweepy
import finalproject_keys
import pandas as pd
from pymongo import MongoClient
import json
from TweetStream import TweetStream

# Authentication
auth = tweepy.OAuthHandler(finalproject_keys.api_key, finalproject_keys.api_secret)
auth.set_access_token(finalproject_keys.access_token, finalproject_keys.access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# Read from data.json file and create a DataFrame
with open('data.json', encoding="utf8") as f:
    tweet_data = json.load(f)

df = pd.DataFrame(tweet_data)

# Convert the Twitter ID to a string
df['id'] = df['id'].astype(str)

# Create the Mongo client
mongo_client = MongoClient('localhost', 27017)

# Set the database to Project
db = mongo_client.Project
collection = db.twitter_feed


# Insert tweet_data into MongoDB
collection.insert_many(tweet_data)

record = collection.find_one()
print(record)

# If you have a specific stream to follow, you can uncomment and use the following
# twitter_stream = tweepy.Stream(api.auth, TweetStream(api, db, df.TwitterHandle.tolist()))
# twitter_stream.filter(track=df.TwitterHandle.tolist(), follow=df.TwitterID.tolist())
