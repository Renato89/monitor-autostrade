#!/bin/bash

$KAFKA_PATH/bin/kafka-console-consumer.sh --topic rilevamenti-targa --from-beginning --bootstrap-server localhost:9092