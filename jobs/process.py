#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Spark App.

spark-submit \
  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \

"""
from __future__ import print_function

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

IS_PY2 = sys.version_info < (3,)
APP_NAME = "Tweet Stream"
BATCH_DURATION = 1  # in seconds

if not IS_PY2:
    os.environ['PYSPARK_PYTHON'] = 'python3'


def create_parser():
    parser = argparse.ArgumentParser(description='ETL for play sessions.')
    parser.add_argument('--input', '-i', help='input path', required=True)
    parser.add_argument('--output', '-o', help='output path', required=True)
    return parser


def create_context():
    spark = (SparkSession
             .builder
             .master('local[2]')
             .config('spark.jars.packages',
                     'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2')
             .appName("Spark Streaming Application")
             .enableHiveSupport()
             .getOrCreate())

    ssc = StreamingContext(spark.sparkContext, BATCH_DURATION)
    return ssc


def main(streaming_context):
    kafka_stream = KafkaUtils.createStream(streaming_context,
                                           'localhost:2181',
                                           'raw-event-streaming-consumer',
                                           {'twitter': 1})

    parsed = kafka_stream.map(lambda k, v: json.loads(v))

    # Count number of tweets in the batch
    count_this_batch = kafka_stream.count().map(lambda x:
                                                ('Tweets this batch: %s' % x))

    return


if __name__ == '__main__':
    parser = create_parser()

    args = parser.parse_args()
    print('Args: ', args)

    ssc = create_context()

    ssc = StreamingContext.getOrCreate('/tmp/%s' % APP_NAME,
                                       lambda: create_context())

    main(ssc)

    ssc.start()
    ssc.awaitTermination(timeout=180)
