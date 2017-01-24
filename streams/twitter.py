# -*- coding: utf-8 -*-
from __future__ import print_function

import json
import os

import tweepy
from confluent_kafka import Producer

consumer_key = os.environ['TWITTER_CONSUMER_KEY']
consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
access_token = os.environ['TWITTER_ACCESS_TOKEN']
access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']

KAFKA_CONF = {'bootstrap.servers': 'localhost:29092'}
LIMIT = 100


def main():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    listener = TwitterStreamListener()
    twitter_stream = tweepy.Stream(auth=api.auth, listener=listener)
    twitter_stream.filter(track=['python'], languages=['en'], async=True)


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self):
        super(TwitterStreamListener, self).__init__()
        self.producer = Producer(**KAFKA_CONF)
        self.count = 0
        self.tweets = []

    def on_data(self, data):
        tweet_dict = json.loads(data)
        self.producer.produce('twitter', tweet_dict['text'].encode('utf-8'))
        self.producer.flush()
        print(tweet_dict)
        self.count += + 1
        return self.count <= LIMIT

    def on_error(self, status_code):
        if status_code == 420:
            return False


if __name__ == '__main__':
    main()
