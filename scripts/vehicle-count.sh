#!/bin/bash
source config/path.config
$KAFKA_PATH/bin/kafka-topics.sh --delete --topic conteggio-per-tratto --bootstrap-server localhost:9092
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 src/vehicle_count.py