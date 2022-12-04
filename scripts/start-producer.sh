#!/bin/bash
source config/path.config
scripts/write-events.sh | $KAFKA_PATH/bin/kafka-console-producer.sh --topic rilevamenti-targa --bootstrap-server localhost:9092