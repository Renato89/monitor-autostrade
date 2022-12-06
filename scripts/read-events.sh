#!/bin/bash
source config/path.config
$KAFKA_PATH/bin/kafka-console-consumer.sh --topic conteggio-per-tratto --bootstrap-server localhost:9092