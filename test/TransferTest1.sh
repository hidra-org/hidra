oldfile=/opt/HiDRA/test_file.cbf
newfile=/opt/HiDRA/data/source/local/test_file.cbf
movedfile=/opt/HiDRA/data/target/local/test_file.cbf

procname=HiDRA_test1


python /opt/HiDRA/src/sender/DataManager.py \
    --configFile /opt/HiDRA/test/dataManager.conf \
    --procname "$procname" \
    --logfileName "${procname}.log" \
    --eventDetectorType "InotifyxDetector" \
    --dataFetcherType "getFromFile" \
    --useDataStream '' \
    --storeData True \
    --removeData True \
    --verbose &

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
