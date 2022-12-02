#!/bin/bash

./write-events.sh | /usr/local/kafka/bin/kafka-console-producer.sh --topic rilevamenti-targa --bootstrap-server localhost:9092