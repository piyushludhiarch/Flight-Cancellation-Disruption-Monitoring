#!/bin/bash

echo "Creating HBase table..."

hbase shell <<'HBASE'
create 'flight_status', 'info'
HBASE

echo "HBase table flight_status created."
