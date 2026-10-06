# Flight Cancellation & Disruption Monitoring

## Overview

Real-time flight disruption monitoring system using Apache Kafka,
Apache Spark Structured Streaming and HBase.

## Architecture

Kafka
  |
  +-- FLIGHT_DELAY
  +-- FLIGHT_CANCELLED
  +-- GATE_CHANGED
  +-- WEATHER_ALERT
              |
              v
      Spark Structured Streaming
              |
              v
      Disruption Correlation
              |
              v
             HBase
              |
              v
       Latest Flight Status

## Example

Flight:

AI501

Delay:

180 minutes

Weather:

Severe

Result:

DISRUPTION_ALERT

## Technologies

- Python
- Apache Kafka
- Apache Spark
- Spark Structured Streaming
- HBase
- HappyBase

## Kafka Events

### FLIGHT_DELAY

Contains:

- flight
- timestamp
- delay_minutes

### FLIGHT_CANCELLED

Contains:

- flight
- timestamp
- reason

### GATE_CHANGED

Contains:

- flight
- timestamp
- new_gate

### WEATHER_ALERT

Contains:

- flight
- timestamp
- severity

## Disruption Rules

Delay >= 120 minutes AND Severe weather:

DISRUPTION_ALERT

Delay >= 120 minutes:

DELAY_ALERT

Severe weather:

WEATHER_ALERT

Cancellation:

CANCELLED

## HBase

Table:

flight_status

Column family:

info

Stored fields:

- flight
- delay_minutes
- weather
- gate
- reason
- status
- alert
