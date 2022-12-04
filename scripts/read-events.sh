#!/bin/bash
source config/path.config
$KAFKA_PATH/bin/kafka-console-consumer.sh --topic ultimi-avvistamenti --bootstrap-server localhost:9092