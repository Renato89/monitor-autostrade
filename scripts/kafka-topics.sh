#!/bin/bash
source config/path.config
$KAFKA_PATH/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list