#/bin/sh

BASEPATH=/home/kuhnm/Arbeit

FILES=${BASEPATH}/test_data/test_015_00001.cbf
TARGET=$BASEPATH/live-viewer/data/source/local
i=1
LIMIT=1000
while [ "$i" -le $LIMIT ]
do
    TARGET_FILE="$TARGET/$i.cbf"
    echo $TARGET_FILE
    cp $FILES "$TARGET_FILE"
    sleep 0.2
    i=$(($i+1))
done
