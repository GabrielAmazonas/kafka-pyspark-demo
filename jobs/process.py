#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Spark App.

spark-submit \
  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \

"""
from __future__ import print_function

import argparse
import os
import sys

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

IS_PY2 = sys.version_info < (3,)
APP_NAME = 'Tweet Stream'
BATCH_DURATION = 1  # in seconds
ZK_HOST = 'localhost:32181'
TOPICS = {'twitter': 1}
GROUP_ID = 'spark-streaming-consumer'

if not IS_PY2:
    os.environ['PYSPARK_PYTHON'] = 'python3'


def create_parser():
    parser = argparse.ArgumentParser(description='ETL for play sessions.')
    parser.add_argument('--input', '-i', help='input path', required=True)
    parser.add_argument('--output', '-o', help='output path', required=True)
    return parser


# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = (SparkSession
                                                      .builder
                                                      .config(conf=sparkConf)
                                                      .enableHiveSupport()
                                                      .getOrCreate())
    return globals()['sparkSessionSingletonInstance']


def create_context():
    spark = (SparkSession
             .builder
             .master('local[2]')
             .config('spark.jars.packages',
                     'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2')
             .appName('Spark Streaming Application')
             .enableHiveSupport()
             .getOrCreate())

    ssc = StreamingContext(spark.sparkContext, BATCH_DURATION)
    return ssc


def main(streaming_context):
    kafka_stream = KafkaUtils.createStream(streaming_context,
                                           zkQuorum=ZK_HOST,
                                           groupId=GROUP_ID,
                                           topics=TOPICS)

    parsed = kafka_stream.map(lambda x: x[1])

    streaming_context.checkpoint('./checkpoint-tweet')

    running_counts = parsed.flatMap(
        lambda line: (line.split(' ')).map(lambda word: (word, 1))
            .updateStateByKey(updateFunc).transform(
            lambda rdd: rdd.sortBy(lambda x: x[1], False)))

    # Count number of tweets in the batch
    count_this_batch = kafka_stream.count().map(lambda x:
                                                ('Tweets this batch: %s' % x))

    return


if __name__ == '__main__':
    conf = (SparkConf()
            .setMaster('local[2]')
            .setAppName('Spark Streaming Application')
            .set('spark.jars.packages',
                 'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2'))

    parser = create_parser()

    args = parser.parse_args()
    print('Args: ', args)

    ssc = create_context()

    ssc = StreamingContext.getOrCreate('/tmp/%s' % APP_NAME, create_context)

    main(ssc)

    ssc.start()
    ssc.awaitTermination(timeout=180)
