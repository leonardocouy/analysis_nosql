import re
import tweepy
from pymongo import MongoClient
from textblob import TextBlob
from decouple import config
from tweepy import Stream
from tweepy import StreamListener
from tweepy.streaming import json

CONSUMER_KEY = config('CONSUMER_KEY')
CONSUMER_SECRET = config('CONSUMER_SECRET')
ACCESS_TOKEN = config('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = config('ACCESS_TOKEN_SECRET')

client = MongoClient('localhost', 27017)
db = client.aula


class TweetStreamListener(StreamListener):

    def on_data(self, data):

        tweet = json.loads(data)

        filtered_tweet = {}

        try:
            # Clean tweet text (Remove links and special characters)
            filtered_tweet["text"] = ' '.join(
                re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)",
                       " ", tweet["text"]).split())

            # Sentiment Analysis
            analysis = TextBlob(filtered_tweet["text"])
            if analysis.sentiment.polarity > 0:
                filtered_tweet["sentiment"] = "positive"
            elif analysis.sentiment.polarity == 0:
                filtered_tweet["sentiment"] = "neutral"
            else:
                filtered_tweet["sentiment"] = "negative"

            # Ignore retweets
            if (not tweet["retweeted"]) and ('RT @' not in tweet["text"]):
                db.tweets.insert(filtered_tweet)
        except KeyError:
            pass

if __name__ == '__main__':
    listener = TweetStreamListener()

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    stream = Stream(auth, listener)
    stream.filter(track=['Lebron James'], async=True)

