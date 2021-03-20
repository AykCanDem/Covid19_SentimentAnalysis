# -*- coding: utf-8 -*-
"""
Created on Sat May 23 09:50:57 2020

@author: Aykut Caner
"""
import sqlite3
import pandas as pd
from geopy import OpenMapQuest
import keys
import folium
import time
from geopy.geocoders import Nominatim



def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        db_name = db_file.split("\\")[-1]
        print(f"Connected to {db_name}")
    except Error as e:
        print(e)

    return conn


def get_geocodes(df_tweets):
    """Get the latitude and longitude for each tweet's location.
    Returns the number of tweets with invalid location data."""
    print('Getting coordinates for tweet locations...')
    
    cursor=connection.cursor()
    geolocator = Nominatim(user_agent="Corona/1.0") #provide app name in format AppName/version
    bad_locations = 0
    none_locations = 0

    for i in range(len(df_tweets)):
        id = df_tweets.at[i,'ID']
        processed = False
        delay = .1  # used if OpenMapQuest times out to delay next call
        print(df_tweets.at[i,'LOCATION'])
        if df_tweets.at[i,'LOCATION'] == None: #if location is not defined, go to next iteration
            print('----')
            none_locations += 1
            continue
        counter = 0
        flag = 0
        while not processed:
            try:  # get coordinates for tweet['location']
                geo_location = geolocator.geocode(df_tweets.at[i,'LOCATION'])
                processed = True
                time.sleep(1)
            except:  # timed out, so wait before trying again
                print('OpenMapQuest service timed out. Waiting.')
                time.sleep(delay)
                counter += 1
                print(counter)
                if counter == 10:
                    counter = 0
                    flag = 1
                    processed = True
                    continue
                delay += .1
        
        if flag == 1:
            flag = 0
            continue
        if geo_location:  
            df_tweets.at[i,'LATITUDE'] = geo_location.latitude
            df_tweets.at[i,'LONGITUDE'] = geo_location.longitude
        else:  
            bad_locations += 1  # df_tweets.at[i,'LOCATION'] was invalid
    
        query = 'UPDATE dbcontent SET LATITUDE = "' + str(df_tweets.at[i,'LATITUDE']) + '" WHERE ID = ' + str(id) + ";"
        print(query)
        cursor.execute(query)
        query = 'UPDATE dbcontent SET LONGITUDE = "' + str(df_tweets.at[i,'LONGITUDE']) + '" WHERE ID = ' + str(id) + ";"
        cursor.execute(query)
        print(query)
        print("---------------------------------------------")        
        
    cursor.close()
   
    print('Done geocoding with:: Bad Locations = ' + str(bad_locations) + ', None Locations = ' + str(none_locations))
    






def marker(df_tweets):
    """the function which marks the locations in the map with color coding sentiment"""
    sentiment_map = folium.Map(location=[53, 9], zoom_start=5, detect_retina=True)
    for tweet in df_tweets.itertuples():
        print(tweet.ID)
        print(tweet.LATITUDE, tweet.LONGITUDE)
        if tweet.LATITUDE == None or tweet.LONGITUDE == None or tweet.LATITUDE == 'None' or tweet.LONGITUDE == 'None':
            continue
        if float(tweet.COMPOUND) > 0:
            markercolor = 'green'
        elif float(tweet.COMPOUND) < 0:
            markercolor = 'red'
        else:
            continue #do not mark neutral tweets
            #markercolor = 'black'
            
            
        if tweet.LANGUAGE not in ['tr','de','nl','fr','es','it','pt','en']:
            continue
            
        
        folium.Circle(
                radius=600,
                location=[float(tweet.LATITUDE) , float(tweet.LONGITUDE)],
                #popup='xxxxxxx',
                color=markercolor,
                fill=True,
                fill_color=markercolor,
                ).add_to(sentiment_map)
        
    sentiment_map.save('tweet_map4.html')
    print('tweet_map.html is saved to current directory')
    return sentiment_map
    
            
        




connection = create_connection(r"C:\Users\Aykut Caner\Desktop\CoronaProject\TweetsDB.db")

df_tweets = pd.read_sql('SELECT * FROM dbcontent', connection)

#get_geocodes(df_tweets)
connection.commit()


marker(df_tweets)


connection.close()


