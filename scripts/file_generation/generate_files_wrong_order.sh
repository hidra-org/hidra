#!/bin/bash

SOURCE=/opt/hidra

TARGET=${SOURCE}/data/source/current/raw/second_test

FILES=${SOURCE}/test/test_files/perkin_elmer_test.tif
#FILES=${SOURCE}/test/test_files/test_file.tif

SCAN=3

for i in {0,1,3,2}
do
    TARGET_FILE="$TARGET/${SCAN}_Test_PE-$(printf %05d $i).tif"
    echo "$TARGET_FILE"
    cp "$FILES" "$TARGET_FILE"
    sleep 1s
done

SCAN=$((SCAN+1))

for i in {0,1}
do
    TARGET_FILE="$TARGET/${SCAN}_Test_PE-$(printf %05d $i).tif"
    echo "$TARGET_FILE"
    cp "$FILES" "$TARGET_FILE"
    sleep 1s
done

