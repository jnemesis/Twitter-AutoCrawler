import tweepy
import datetime
import time
import pandas

#Import json containing developer key, comment out if done manual
import json

with open('ENTER json LOCATION HERE') as f:
  data = json.load(f)

consumer_key = data["consumer_key"]
consumer_secret = data["consumer_secret"]
access_token = data["access_token"]
access_token_secret = data["access_token_secret"]

#If not using json for developer key, manually enter developer info here and uncomment below
#consumer_key = 'change with your consumer key'
#consumer_secret = 'change with your consumer secret'
#access_token = 'change with your access token'
#access_token_secret = 'change with your access secret'

#Enter tweet query limits, hashtag keyword(s), and geocoord (lat,lon,radius)
tweetsPerQry = 100
maxTweets = 10000
hashtag = "#wildfire -filter:retweets"
geocoordinate = "34.499766,-110.081393,300km"

#Authenticate dev key then runs the seach loop
authentication = tweepy.OAuthHandler(consumer_key, consumer_secret)
authentication.set_access_token(access_token, access_token_secret)
api = tweepy.API(authentication, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
while True:
    maxId = -1
    tweetCount = 0
    now=datetime.datetime.now()
    dt_string = now.strftime("%d-%m-%Y %H %M %S")
    print(dt_string)
    
    while tweetCount < maxTweets:
            if(maxId <= 0):
                newTweets = api.search(q=hashtag, geocode=geocoordinate, count=tweetsPerQry, result_type="recent", tweet_mode="extended")
            else:
                newTweets = api.search(q=hashtag, geocode=geocoordinate, count=tweetsPerQry, max_id=str(maxId - 1), result_type="recent", tweet_mode="extended")
        
            if not newTweets:
                print("No More Tweets")
                break
        
            for tweet in newTweets:
                tweet_log = [[tweet.user.screen_name, tweet.user.location, tweet.created_at,tweet.full_text.encode('utf-8')] for tweet in newTweets]
            tweetCount += len(newTweets)	
            maxId = newTweets[-1].id

    print ("New csv posted for "+dt_string)

    #Posts tweets to a data frame then sends to csv.  
    twt = pandas.DataFrame(data=tweet_log, columns=['user', "location", "when created", "tweet text"])
    twt.to_csv('Tweets of interest.csv',mode='a', index=False, header=False)
    
    #Re-opens csv into dataframe and removes duplicates before returning to csv
    df = pandas.read_csv('Tweets of interest.csv')
    df_clean = df.drop_duplicates(subset=['tweet text'])
    df_clean.to_csv('Tweets of interest.csv', index=False)
    
    #set desired delay between searches below (in seconds) 
    time.sleep(600)