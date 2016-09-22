SCRIPTPATH=$(readlink -f $0)
BASEPATH=${SCRIPTPATH%%/test/TransferTest3.sh}
echo $BASEPATH


oldfile=$BASEPATH/test_file.cbf
newfile=$BASEPATH/data/source/local/test_file.cbf
movedfile=$BASEPATH/data/target/local/test_file.cbf

procname=HiDRA_test3


python $BASEPATH/src/sender/DataManager.py \
    --configFile $BASEPATH/test/dataManager.conf \
    --procname $procname \
    --logfilePath $BASEPATH/logs \
    --logfileName ${procname}.log \
    --monitoredDir $BASEPATH/data/source \
    --localTarget $BASEPATH/data/target/local \
    --eventDetectorType HttpDetector \
    --dataFetcherType getFromHttp \
    --eigerIp 131.169.55.170 \
    --eigerApiVersion 1.5.0 \
    --historySize 2\
    --useDataStream '' \
    --storeData True \
    --removeData '' \
    --verbose &

sleep 5

if cmp -s "$oldfile" "$movedfile" ; then
   echo "Files identical"
else
   echo "Files not identical"
fi

rm $movedfile

killall $procname
