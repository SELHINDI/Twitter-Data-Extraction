
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:36:01 2023

@author: ginac
"""


import pandas as pd
from pymongo import MongoClient
import json
from pprint import pprint

#create data frame and read from file

mongo_client= MongoClient('localhost', 27017)
db = mongo_client.Project
collection = db.twitter_feed


print("Query1 - top 10 twitter users by park")
record = collection.aggregate(
    [{"$group": {"_id": "$TwitterHandle", "CountByPark": {"$sum":1}}}
     ,{"$sort": {"CountByPark":-1}},
     {"$limit":10}])
for r in record:
    print(r)

print("\n\nQuery 2 - parks with a high follower count")

record = collection.find(
    {"user.followers_count": {"$gte": 100000}},
    {"user.screen_name":1, "TwitterHandle":1, "user.followers_count":1})
for r in record:
    print(r)

print("\n\nQuery 3 - number of retweets")
record = collection.count_documents(
    {"retweet_count": {"$gte":1}
     })

print(record)


print("\n\nQuery 4 - search through text for a particular word")
record = collection.find(
    {"text":{"$regex":"picture"}}, {"TwitterHandle":1, "text":1})
for r in record:
    print(r)


print("\n\nQuery 5 - your own query")
record = collection.find(
    {"text":{"$regex":"water"}}, {"TwitterHandle":1, "text":1})
for r in record:
    print(r)
