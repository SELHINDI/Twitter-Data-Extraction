# -*- coding: utf-8 -*-
"""
Created on Sat Nov  7 21:34:00 2020

@author: Student
"""

from geopy import OpenMapQuest
from geopy import Nominatim
import finalproject_keys
from finalproject_statecodes import state_codes
import folium
from pymongo import MongoClient
import pandas as pd

geolocator = Nominatim(user_agent= "myapp")
#ourMap = OpenMapQuest(api_key =finalproject_keys.mapquest_key)
############################################
#write code in this comment box
#create data frame and read from file

df = pd.read_csv("nationalparks.csv")
#create the mongo client
mongo_client = MongoClient('Localhost',27017)
#set the database to Project
db = mongo_client.Project
#create a list named tweets
tweets = []
#create a list named park_locations
park_locations = []



############################################

#use the count_documents method to count the total number of documents in the collection that contains that 
#text for each park
for np in df.TwitterHandle:
    tweets.append(db.twitter_feed.count_documents({"$text": {"$search": np}}))
park_df = df.assign(Tweets = tweets)

#get the states for each park
states = park_df.State.unique()
states.sort()

#Must append a command USA after each state in the list so that the map has accurate location information
for s in states:
    park_locations.append(geolocator.geocode(state_codes[s] + ', USA'))
    
#create the map - feel free to adjust location and zoom   
usmap = folium.Map(location=[39.98334, -82.9833], zoom_start =4)

#This for loop walks through each item in the collection and returns the data for each park
#It then creates a marker with the latitude and longitude of the location. 
#Then add the marker to the map
for index, (name, group) in enumerate(park_df.groupby('State')):
    text = [state_codes[name]]
            
    for s in group.itertuples():
        text.append(f'{s.Name}, {s.Area}, {s.YearEstablished} Tweets: {s.Tweets}')
    display = '<br>'.join(text)
    marker = folium.Marker((park_locations[index].latitude, park_locations[index].longitude), popup = display)
    marker.add_to(usmap)


#save to a file named national_park_tweets.html
usmap.save("national_park_tweets.html")
            