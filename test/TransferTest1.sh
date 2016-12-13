# tests file moving on basis of inotifyx

SCRIPTPATH=$(readlink -f $0)
BASEPATH=${SCRIPTPATH%%/test/TransferTest1.sh}

oldfile=$BASEPATH/test_file.cbf
newfile=$BASEPATH/data/source/local/test_file.cbf
movedfile=$BASEPATH/data/target/local/test_file.cbf

procname=HiDRA_test1


python $BASEPATH/src/sender/datamanager.py \
    --config_file $BASEPATH/test/datamanager.conf \
    --procname $procname \
    --log_path $BASEPATH/logs \
    --log_name ${procname}.log \
    --monitored_dir $BASEPATH/data/source \
    --local_target $BASEPATH/data/target \
    --ext_ip 0.0.0.0 \
    --event_detector_type inotifyx_events \
    --data_fetcher_type file_fetcher \
    --use_data_stream False \
    --store_data True \
    --remove_data True \
    --verbose \
    --onscreen debug &

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
rm $BASEPATH/logs/${procname}.log*

killall $procname

sleep 1
