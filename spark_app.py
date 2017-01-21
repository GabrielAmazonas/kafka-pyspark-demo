#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Spark App.

spark-submit \
      --packages \

"""
import os
import sys
import json
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

IS_PY2 = sys.version_info < (3,)
APP_NAME = "StreamingTest"
BATCH_DURATION = 1  # in seconds

if not IS_PY2:
    os.environ['PYSPARK_PYTHON'] = 'python3'


def create_parser():
    parser = argparse.ArgumentParser(description='ETL for play sessions.')
    parser.add_argument('--input', '-i', help='input path', required=True)
    parser.add_argument('--output', '-o', help='output path', required=True)
    return parser


def create_session():
    spark = (SparkSession
             .builder
             .master('local[2]')
             .config('spark.jars.packages',
                     'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2')
             .appName("Spark Streaming Application")
             .enableHiveSupport()
             .getOrCreate())

    return spark


def main(spark_session):
    pass


if __name__ == '__main__':
    parser = create_parser()

    args = parser.parse_args()
    print('Args: ', args)

    spark = create_session()
    ssc = StreamingContext(spark.sparkContext, BATCH_DURATION)
    kafka_stream = KafkaUtils.createStream(ssc,
                                           'localhost:2181',
                                           'raw-event-streaming-consumer',
                                           {'pageviews': 1})
    parsed = kafka_stream.map(lambda k, v: json.loads(v))

    main(spark)

    ssc.start()
    ssc.awaitTermination()
