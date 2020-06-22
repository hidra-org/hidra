#!/bin/bash

SOURCE=/opt/hidra

TARGET=${SOURCE}/data/source/current/raw/second_test
#LOWER_LIMIT=0
#UPPER_LIMIT=9
LOWER_LIMIT=10
UPPER_LIMIT=19

FILES=${SOURCE}/test/test_files/perkin_elmer_test.tif
#FILES=${SOURCE}/test/test_files/test_file.tif

SCAN=1

for i in $(seq -f "%05g" $LOWER_LIMIT $UPPER_LIMIT)
#while [ "$i" -le "$UPPER_LIMIT" ]
do
    TARGET_FILE="$TARGET/${SCAN}_Test_PE-$i.tif"
    echo "$TARGET_FILE"
    cp "$FILES" "$TARGET_FILE"
    sleep 0.2s
    #i=$((i+1))
done
