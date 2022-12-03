#!/bin/bash

# Start daemon.
# Comunque non funziona perchè i tre processi 
# non terminano e il comando && aspetta la terminazione 
# di quello precedente.
echo "Starting Zookeeper";
nohup ./start-zookeeper.sh  & sleep 5 & \
echo "Starting Kafka";
nohup ./start-kafka.sh & sleep 10 & \
echo "Sending events";
nohup ./start-producer.sh 