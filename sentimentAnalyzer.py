# -*- coding: utf-8 -*-
"""
Created on Thu May 21 09:17:57 2020

@author: Aykut Caner
"""
from textblob import TextBlob
import sqlite3
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import preprocessor as p
import time
import demoji

def start_sentiment_analysis(connection, df_tweets):
    """ A function that uses NLTK SentimentIntensityAnalyzer to analze tweet's sentiment """
    
    sid = SentimentIntensityAnalyzer()
    cursor = connection.cursor()
    for i in range(len(df_tweets)):
        id = df_tweets.loc[i,'ID']
        dict_results = sid.polarity_scores(df_tweets.at[i,'FCONTENT']) 
       
        df_tweets.at[i,'NEGATIVITY'] = dict_results['neg']
        df_tweets.at[i,'NEUTRALITY'] = dict_results['neu']
        df_tweets.at[i,'POSITIVITY'] = dict_results['pos']
        df_tweets.at[i,'COMPOUND'] = dict_results['compound']
        
        #write changes to database
        query = "UPDATE GoldenSet SET NEGATIVITY = " + str(df_tweets.at[i,'NEGATIVITY']) + " WHERE ID = " + str(id) + ";"
        cursor.execute(query)
        query = "UPDATE GoldenSet SET NEUTRALITY = " + str(df_tweets.at[i,'NEUTRALITY']) + " WHERE ID = " + str(id) + ";"
        cursor.execute(query)
        query = "UPDATE GoldenSet SET POSITIVITY = " + str(df_tweets.at[i,'POSITIVITY']) + " WHERE ID = " + str(id) + ";"
        cursor.execute(query)
        query = "UPDATE GoldenSet SET COMPOUND = " + str(df_tweets.at[i,'COMPOUND']) + " WHERE ID = " + str(id) + ";"
        cursor.execute(query)
        print(df_tweets.at[i,'FCONTENT'])
        print(dict_results)
        print('-'*40 + '\n')

    cursor.close()
    print("\n\n\n----------------------- SENTIMENT ANALYSIS FINISHED -------------------------\n\n\n")



    

def translate(text):
    """ Google Translate API translate request """
    blob = TextBlob(text);
    try:
        tr_text = blob.translate()
    except Exception as e:
        print(e)
        return text
    return str(tr_text)
    

def start_translate(connection, df_tweets):
    """ A function that translate non-english tweets to english"""
    
    cursor = connection.cursor()
    for i in range(len(df_tweets)):
        #if language is english or undefined, continue with next tweet
        if df_tweets.loc[i,'LANGUAGE'] in ['en','und']:
            continue 
        print("ORIGINAL: ",df_tweets.loc[i,'CONTENT'])
        id = df_tweets.loc[i,'ID']
        tr_text = translate(df_tweets.at[i,'CONTENT'])
        #change the character " to ' to prevent quote error when writing to database
        print("TRANSLATED: ", tr_text.replace('"',"'"))
        
        #before writing the translated content to the database,
        #replace " with ' in order to prevent SQL Syntax error in UPDATE statement
        df_tweets.at[i,'TRCONTENT'] = tr_text.replace('"',"'")

        
        #write changes to database
        query = 'UPDATE GoldenSet SET TRCONTENT = "' + str(df_tweets.at[i,'TRCONTENT']) + '" WHERE ID = ' + str(id) + ";"
        cursor.execute(query)
        print('-'*40 + '\n')
        time.sleep(1) #sleep 1 sec to prevent lower the frequency of requests to translate API.
    cursor.close()
    print("\n\n\n----------------------- TRANSLATE FINISHED -------------------------\n\n\n")
    
    
    
    
def clean_data(connection, df_tweets):
    """ A function that cleans tweets from URLs; Reserved keywords like RT,FAV; """
    
    cursor = connection.cursor()
    for i in range(len(df_tweets)):
        print("ORIGINAL: ",df_tweets.loc[i,'CONTENT'])
        id = df_tweets.loc[i,'ID']
        
        p.set_options(p.OPT.URL, p.OPT.RESERVED)
        
        cleaned_content = p.clean(df_tweets.loc[i,'CONTENT'])
        #change the character " to ' to prevent quote error when writing to database
        cleaned_content = cleaned_content.replace('"',"'")
        df_tweets.at[i,'CONTENT'] = cleaned_content
        
        print("CLEANED: ",df_tweets.at[i,'CONTENT'] )

        #write changes to database
        query = 'UPDATE GoldenSet SET CONTENT = "' + str(df_tweets.at[i,'CONTENT']) + '" WHERE ID = ' + str(id) + ";"
        cursor.execute(query)
        print('-'*40 + '\n')
    cursor.close()
    print("\n\n\n----------------------- CLEANING DATA FINISHED -------------------------\n\n\n")




def emoji2text(connection, df_tweets):
    """ A function that replaces emoji with corresponding text inside the tweets"""
    
    cursor = connection.cursor()
    for i in range(len(df_tweets)):
        id = df_tweets.loc[i,'ID']
        
        content = df_tweets.loc[i,'TRCONTENT'] if df_tweets.loc[i,'TRCONTENT'] else df_tweets.loc[i,'CONTENT']
        
        #find the emojis inside the text, function returns emojis and corresponding text value
        emoDict = demoji.findall(content)
        convertedText = df_tweets.loc[i,'FCONTENT']
        #replace all emojis with corresponding text
        if len(emoDict):
            print('ORIGINAL: ', content)
            for emo,emoText in emoDict.items():
                emoText = ' ' + emoText + ' ' #leading and panding spaces to separate emoji from other words
                convertedText = convertedText.replace(emo,emoText) 
            df_tweets.loc[i,'FCONTENT'] = convertedText
            print('CONVERTED: ', df_tweets.loc[i,'FCONTENT'])
            
            query = 'UPDATE GoldenSet SET FCONTENT = "' + str(df_tweets.at[i,'FCONTENT']) + '" WHERE ID = ' + str(id) + ";"
            cursor.execute(query)
            print('-'*40 + '\n')
        
    cursor.close()
    print("\n\n\n----------------------- EMOJI TO TEXT FINISHED -------------------------\n\n\n")            
    
    
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        db_name = db_file.split("\\")[-1]
        print(f"Connected to {db_name}")
    except Exception as e:
        print(e)

    return conn


connection = create_connection(r"C:\Users\Aykut Caner\Desktop\CoronaProject\CloneDB2")

df_tweets = pd.read_sql('SELECT * FROM GoldenSet WHERE ID < 940', connection)

#df_tweets = pd.read_sql('SELECT * FROM GoldenSet WHERE ID = 317', connection)

clean_data(connection, df_tweets)
start_translate(connection, df_tweets)
emoji2text(connection,df_tweets)
start_sentiment_analysis(connection, df_tweets)

print('Changes committed to Database')
connection.commit()
print('Database connection closed.')
connection.close();


#print(df_tweets)
