#/bin/sh

FILES=/space/test_data/flat/*
TARGET=/space/projects/live-viewer/source/local
for f in $FILES
do
    echo $f
    cp $f $TARGET
done
