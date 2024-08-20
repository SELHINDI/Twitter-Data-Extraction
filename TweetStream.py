# -*- coding: utf-8 -*-
"""
Created on Fri Nov  6 18:53:21 2020

@author: Student
"""

import tweepy
import json

class TweetStream(tweepy.StreamListener):
 

    def __init__(self, api, database, tList):
        
        self.db = database
        self.tweet_count = 0
        self.twitterList = tList
        super().__init__(api)  # call superclass's init

    def on_connect(self):
        #called on a successful connection
        print('Connection successful\n')
    
    def on_error(self, status_code):
        #if an error occurs display the error code
        print("An error as occurred: "+repr(status_code))
        return False

    def on_data(self, data):
        self.tweet_count+=1 #keep track of number of tweets
        # get the tweet text
        try:
            data_json = json.loads(data)
            handle="none"
            for tw in self.twitterList:
                if tw in str(data_json):
                    handle = tw
            newKey = {"TwitterHandle": handle}
            data_json.update(newKey)
            self.db.twitter_feed.insert_one(data_json)
            
    
            print(f'Screen name: {data_json["user"]["name"]}:')
            print(f'   Created: {data_json["created_at"]}')
            print(f'     Tweets Received: {self.tweet_count}')
        except Exception as e:
            print(str(e))
            

        

      
     

        # if reach 1000 tweets return
        return self.tweet_count != 500