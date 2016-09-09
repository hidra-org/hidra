oldfile=/opt/HiDRA/test_file.cbf
newfile=/opt/HiDRA/data/source/local/test_file.cbf
movedfile=/opt/HiDRA/data/target/local/test_file.cbf

procname=HiDRA_test3


python /opt/HiDRA/src/sender/DataManager.py \
    --configFile /opt/HiDRA/test/dataManager.conf \
    --procname "$procname" \
    --logfileName "${procname}.log" \
    --eventDetectorType "HttpDetector" \
    --dataFetcherType "getFromHttp" \
    --eigerIp "131.169.55.170" \
    --eigerApiVersion "1.5.0" \
    --historySize 2\
    --localTarget "/opt/HiDRA/data/target/local" \
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
