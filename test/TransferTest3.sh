# tests moving files gotten via HTTP

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
    --extIp 0.0.0.0 \
    --eventDetectorType http_detector \
    --dataFetcherType http_fetcher \
    --eigerIp 131.169.55.170 \
    --eigerApiVersion 1.5.0 \
    --historySize 2\
    --useDataStream False \
    --storeData True \
    --removeData False \
    --verbose \
    --onScreen debug &

sleep 5

if cmp -s "$oldfile" "$movedfile" ; then
   echo "Files identical"
else
   echo "Files not identical"
fi

rm $movedfile
rm $BASEPATH/logs/${procname}.log*

killall $procname

sleep 1
