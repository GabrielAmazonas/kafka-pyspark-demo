#!/usr/bin/env bash

# create twitter Kafka topic if none exist
docker run \
  --net=host \
  --rm confluentinc/cp-kafka:3.1.1 \
  kafka-topics --create --topic twitter --partitions 1 --replication-factor 1 \
    --if-not-exists --zookeeper localhost:32181

docker run \
  --net=host \
  --rm confluentinc/cp-kafka:3.1.1 \
  kafka-topics --describe --topic twitter --zookeeper localhost:32181


docker run \
  --net=host \
  --rm \
  confluentinc/cp-kafka:3.1.1 \
  kafka-console-consumer --bootstrap-server localhost:29092 --topic twitter \
    --new-consumer --from-beginning --max-messages 42