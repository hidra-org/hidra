SCRIPTPATH=$(readlink -f $0)
BASEPATH=${SCRIPTPATH%%/test/TransferTest1.sh}

oldfile=$BASEPATH/test_file.cbf
newfile=$BASEPATH/data/source/local/test_file.cbf
movedfile=$BASEPATH/data/target/local/test_file.cbf

procname=HiDRA_test1


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
    --useDataStream '' \
    --storeData True \
    --removeData True \
    --verbose \
    --onScreen debug &

sleep 2

cp $oldfile $newfile
echo "Copy done"

sleep 2

if cmp -s "$oldfile" "$movedfile" ; then
   echo "Files identical"
else
   echo "Files not identical"
fi

rm $movedfile

killall $procname
