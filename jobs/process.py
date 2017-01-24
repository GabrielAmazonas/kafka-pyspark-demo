#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Spark Streaming Twitter.

spark-submit \
  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2

"""
from __future__ import print_function

import os
import sys
import json
import argparse

from pyspark import Row
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

IS_PY2 = sys.version_info < (3,)
APP_NAME = 'TwitterStreamKafka'
BATCH_DURATION = 1  # in seconds
ZK_QUORUM = 'localhost:32181'
GROUP_ID = 'spark-streaming-consumer'
TOPICS = {'twitter': 1}
CHECKPOINT_DIRECTORY = '/tmp/%s' % APP_NAME
STREAM_CONTEXT_TIMEOUT = 180  # seconds

SPARK_CONF = (SparkConf()
              .setMaster('local[2]')
              .setAppName(APP_NAME))

if not IS_PY2:
    os.environ['PYSPARK_PYTHON'] = 'python3'


def create_parser():
    parser = argparse.ArgumentParser(description=APP_NAME)
    return parser


def get_hashtags(tweet):
    return [hashtag['text'] for hashtag in tweet['entities']['hashtags']]


def get_session(spark_conf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = (SparkSession
                                                      .builder
                                                      .config(conf=spark_conf)
                                                      .enableHiveSupport()
                                                      .getOrCreate())
    return globals()['sparkSessionSingletonInstance']


def create_context():
    spark_session = get_session(SPARK_CONF)
    ssc = StreamingContext(spark_session.sparkContext, BATCH_DURATION)
    ssc.checkpoint(CHECKPOINT_DIRECTORY)
    return ssc


def process(timestamp, rdd):
    print("========= %s =========" % str(timestamp))
    # Get the singleton instance of SparkSession
    spark = get_session(rdd.context.getConf())

    # Convert RDD[List[String]] to RDD[Row] to DataFrame
    rows = rdd.flatMap(lambda w: Row(word=w))
    words_df = spark.createDataFrame(rows)

    # Creates a temporary view using the DataFrame
    words_df.createOrReplaceTempView('words')

    # Do word count on table using SQL and print it
    sql = "SELECT word, COUNT(1) AS total FROM words GROUP BY word"
    word_count_df = spark.sql(sql)
    word_count_df.show()


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    print('Args: ', args)

    ssc = StreamingContext.getOrCreate(CHECKPOINT_DIRECTORY, create_context)
    stream = KafkaUtils.createStream(ssc, ZK_QUORUM, GROUP_ID, TOPICS)

    # Count number of tweets in the batch
    count_batch = stream.count().map(lambda x: ('Num tweets: %s' % x))

    hashtags = (stream
                .mapValues(json.loads)
                .map(get_hashtags))

    hashtags.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination(timeout=STREAM_CONTEXT_TIMEOUT)
