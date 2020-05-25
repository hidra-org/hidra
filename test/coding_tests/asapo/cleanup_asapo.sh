#!/bin/bash

HOST=localhost
#HOST=asapo-services

# default:
# echo "db.dropDatabase()" | mongo --host $HOST asapo_test_detector

echo "db.dropDatabase()" | mongo --host $HOST asapo_test_hidra_test
echo "db.dropDatabase()" | mongo --host $HOST asapo_test_asapo_test
#echo "db.dropDatabase()" | mongo --host $HOST asapo_test_hidra_test2
#echo "db.dropDatabase()" | mongo --host $HOST asapo_test_hidra_test_scan_000
#echo "db.dropDatabase()" | mongo --host $HOST asapo_test_zmq_target
#echo "db.dropDatabase()" | mongo --host $HOST asapo_test_zmq_target_local

# check which databases are available:
# echo "show dbs" | mongo --host $HOST