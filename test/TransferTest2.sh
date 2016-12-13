# tests file moving on basis of watchdog

SCRIPTPATH=$(readlink -f $0)
BASEPATH=${SCRIPTPATH%%/test/TransferTest2.sh}

oldfile=$BASEPATH/test_file.cbf
newfile=$BASEPATH/data/source/local/test_file.cbf
movedfile=$BASEPATH/data/target/local/test_file.cbf

procname=HiDRA_test2


python $BASEPATH/src/sender/DataManager.py \
    --config_file $BASEPATH/test/dataManager.conf \
    --procname $procname \
    --log_path $BASEPATH/logs \
    --log_name ${procname}.log \
    --monitored_dir $BASEPATH/data/source \
    --local_target $BASEPATH/data/target \
    --ext_ip 0.0.0.0 \
    --event_detector_type watchdog_events \
    --data_fetcher_type file_fetcher \
    --action_time 2 \
    --time_till_closed 1 \
    --use_data_stream False \
    --store_data True \
    --remove_data True \
    --verbose \
    --onscreen debug &

sleep 2

cp $oldfile $newfile
echo "Copy done"

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
