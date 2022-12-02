#!/bin/bash

/usr/local/kafka/bin/kafka-topics.sh --create --topic rilevamenti-targa --bootstrap-server localhost:9092
/usr/local/kafka/bin/kafka-topics.sh --create --topic ultimi-avvistamenti --bootstrap-server localhost:9092
/usr/local/kafka/bin/kafka-topics.sh --create --topic tempi-di-percorrenza --bootstrap-server localhost:9092
