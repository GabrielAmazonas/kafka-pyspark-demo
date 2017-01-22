# -*- coding: utf-8 -*-
from __future__ import print_function

import os
import json

import tweepy
from confluent_kafka import Producer

consumer_key = os.environ['TWITTER_CONSUMER_KEY']
consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
access_token = os.environ['TWITTER_ACCESS_TOKEN']
access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']

LIMIT = 100


def main():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    stream = TwitterStreamListener()
    twitter_stream = tweepy.Stream(auth=api.auth, listener=stream)
    twitter_stream.filter(track=['python'], languages=['en'], async=True)


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self):
        super(TwitterStreamListener, self).__init__()
        self.producer = Producer({'bootstrap.servers': 'docker:9092'})
        # value_serializer=lambda v: json.dumps(v)
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
