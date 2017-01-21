# -*- coding: utf-8 -*-
from __future__ import print_function

import tweepy
import json


def initialize():
    auth = tweepy.OAuthHandler("xxxxxx", "xxxxxxxxxx")
    auth.set_access_token("xxxxxxxxxx", "xxxxxxxxxxx")
    api = tweepy.API(auth)

    stream = TwitterStreamListener()
    twitter_stream = tweepy.Stream(auth=api.auth, listener=stream)
    # twitter_stream.filter(track=['iphone'], async=True)
    twitter_stream.sample(async=True)


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self, api, numtweets=0):
        self.api = api
        self.count = 0
        self.limit = int(numtweets)
        super(TwitterStreamListener, self).__init__()

    def on_data(self, tweet):
        tweet_data = json.loads(tweet)
        if 'text' in tweet_data and tweet_data['user']['lang'] == 'en':
            print(tweet_data['text'].encode('utf-8').rstrip())
            self.count += + 1
        return self.count != self.limit

    def on_error(self, status_code):
        if status_code == 420:
            return False


if __name__ == '__main__':
    pass
