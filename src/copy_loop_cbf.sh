#/bin/sh

BASEPATH=/home/p11user/live-viewer

FILES=/tmp/PhilipPBS_3_00001.cbf
#FILES=${BASEPATH}/test_015_00001.cbf
TARGET=/rd/local/sender_test
i=1
LIMIT=1000
while [ "$i" -le $LIMIT ]
do
    TARGET_FILE="$TARGET/$i.cbf"
    echo $TARGET_FILE
    cp $FILES "$TARGET_FILE"
#    sleep 0.2
    i=$(($i+1))
done
