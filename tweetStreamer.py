# -*- coding: utf-8 -*-
"""
Created on Tue May 19 22:36:53 2020

@author: Aykut Caner
"""

import tweepy
import keys
import sqlite3
import winsound

class TweetListener(tweepy.StreamListener):
    """ A class that inheretred from Tweepy.StreamListener to initialize streaming and processing retrieved tweets """
    def __init__(self,api,connection, limit=10 ):
        self.tweet_count = 0
        self.TWEET_LIMIT = limit
        self.connection = connection
        super().__init__(api)

    def on_status(self, status):
        
        # ignore retweets 
        if status.text.startswith('RT'):
            return
        
        #create a dictionary that keeps the current tweet
        field = {}
        field['created_at'] = status.created_at
        field['screen_name'] = status.user.screen_name
        field['language'] = status.lang
        field['location'] = status.user.location

        try:                    
            #try to get extended tweet
            tweet_text = status.extended_tweet['full_text']
            field['text'] = status.extended_tweet['full_text']
        except Exception as er:
            #if it is not extended, get the normal tweet
            #print(er)
            tweet_text = status.text
            field['text'] = status.text
        

        #print details of the tweet    
        print(f'Created at: {field["created_at"]}')
        print(f'Screen name: {field["screen_name"]}')
        print(f'Language: {field["language"]}')
        print(f'Location: {field["location"]}')
        print(f'{tweet_text}')
        print('-'*10 + '\n')
        
        
        #ignore the tweets that do not contain track words
        track=['corona','COVID-19','covid','coronavirus']
        accept='no'
        for word in track:
            if word in field['text'].lower():
                accept = 'yes'
                break
        if accept == 'no':
            return

        # insert tweet details to database
        cursor = self.connection.cursor()
        cursor = cursor.execute('''INSERT INTO TweetsDB (TIMESTAMP,USER,LANGUAGE,LOCATION,CONTENT)
                                VALUES (?,?,?,?,?);''', (field["created_at"], field["screen_name"], field["language"], field["location"],field['text']))

        connection.commit()
        self.tweet_count += 1
        
        # return false when reached tweet limit
        return self.tweet_count < self.TWEET_LIMIT
    
    def on_limit(self, track):
        print("Reached rate limit.")
        
    def on_error(self, status_code):
        print(f"Error with status code: {status_code}")
        return False


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


def authenticate():
    """ A function that authenticate the user and returns tweepy API """
    
    #keys.py is a document where API keys are stored.
    auth = tweepy.OAuthHandler(keys.consumer_key, keys.consumer_secret)
    auth.set_access_token(keys.access_token, keys.access_token_secret)
    api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api



connection = create_connection(r"C:\Users\Aykut Caner\Desktop\CoronaProject\CloneDB")

api = authenticate()    

tweet_listener = TweetListener(api,connection, limit=10)

stream = tweepy.Stream(api.auth,tweet_listener)


try:
    stream.filter(track=['corona','COVID-19','covid','coronavirus'],languages=['tr','de','nl','fr','es','it','pt'], is_async=False)
except Exception as e:
    # If streaming stops, make a sound.
    print(e)
    frequency = 2500  # Set Frequency To 2500 Hertz
    duration = 1000  # Set Duration To 1000 ms == 1 second
    winsound.Beep(frequency, duration)
    print(connection.total_changes)

#close database connection by printing total number of changes
print(connection.total_changes)
connection.close()

