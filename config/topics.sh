#!/bin/bash

KAFKA_BIN="$HOME/kafka/bin/kafka-topics.sh"

$KAFKA_BIN --create --if-not-exists \
    --topic FLIGHT_DELAY \
    --bootstrap-server localhost:9092

$KAFKA_BIN --create --if-not-exists \
    --topic FLIGHT_CANCELLED \
    --bootstrap-server localhost:9092

$KAFKA_BIN --create --if-not-exists \
    --topic GATE_CHANGED \
    --bootstrap-server localhost:9092

$KAFKA_BIN --create --if-not-exists \
    --topic WEATHER_ALERT \
    --bootstrap-server localhost:9092

echo "Kafka topics created."
