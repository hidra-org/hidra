# tests file moving on basis of inotifyx while OnDA is asking to get the files as well

SCRIPTPATH=$(readlink -f $0)
BASEPATH=${SCRIPTPATH%%/test/TransferTest4.sh}

oldfile=$BASEPATH/test_file.cbf
newfile=$BASEPATH/data/source/local/test_file.cbf
movedfile=$BASEPATH/data/target/local/test_file.cbf

procname=HiDRA_test4
onda_procname=example_onda


python $BASEPATH/src/sender/DataManager.py \
    --configFile $BASEPATH/test/dataManager.conf \
    --procname $procname \
    --logfilePath $BASEPATH/logs \
    --logfileName ${procname}.log \
    --monitoredDir $BASEPATH/data/source \
    --localTarget $BASEPATH/data/target \
    --extIp 0.0.0.0 \
    --eventDetectorType InotifyxDetector \
    --dataFetcherType getFromFile \
    --useDataStream False \
    --storeData True \
    --removeData True \
    --verbose \
    --onScreen debug &

sleep 2

python $BASEPATH/test/API/example_onda.py \
    --procname $onda_procname &

sleep 1

cp $oldfile $newfile
echo "Copy done"

sleep 2

numberOfErrors=$(less $BASEPATH/logs/${procname}.log* | grep "ERROR" | wc -l)
exitstatus=0

if [ $numberOfErrors != 0 ]; then
    echo "Errors in transfer"
    less $BASEPATH/logs/${procname}.log* | grep "ERROR"
    exitstatus=1
else
    echo "No errors in transfer"
fi


if cmp -s "$oldfile" "$movedfile" ; then
   echo "Files identical"
else
   echo "Files not identical"
fi

rm $movedfile

killall $procname
killall $onda_procname
rm $BASEPATH/logs/${procname}.log*

sleep 1

if [ $exitstatus != 0 ]; then
    exit $exitstatus
fi
