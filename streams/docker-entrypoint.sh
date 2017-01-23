#!/usr/bin/env bash

# create twitter Kafka topic if none exist
docker run \
  --net=host \
  --rm confluentinc/cp-kafka:3.1.1 \
  kafka-topics --create --topic twitter --partitions 1 --replication-factor 1 \
    --if-not-exists --zookeeper localhost:32181
