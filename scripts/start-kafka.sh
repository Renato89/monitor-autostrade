#!/bin/bash
source config/path.config
$KAFKA_PATH/bin/kafka-server-start.sh  $KAFKA_PATH/config/server.properties
