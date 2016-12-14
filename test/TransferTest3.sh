# tests moving files gotten via HTTP

SCRIPTPATH=$(readlink -f $0)
BASEPATH=${SCRIPTPATH%%/test/TransferTest3.sh}
echo $BASEPATH


oldfile=$BASEPATH/test_file.cbf
newfile=$BASEPATH/data/source/local/test_file.cbf
movedfile=$BASEPATH/data/target/local/test_file.cbf

procname=HiDRA_test3


python $BASEPATH/src/sender/datamanager.py \
    --config_file $BASEPATH/test/datamanager.conf \
    --procname $procname \
    --log_path $BASEPATH/logs \
    --log_name ${procname}.log \
    --monitored_dir $BASEPATH/data/source \
    --local_target $BASEPATH/data/target/local \
    --ext_ip 0.0.0.0 \
    --event_detector_type http_events \
    --data_fetcher_type http_fetcher \
    --eiger_ip 131.169.55.170 \
    --eiger_api_version 1.5.0 \
    --history_size 2\
    --use_data_stream False \
    --store_data True \
    --remove_data False \
    --verbose \
    --onscreen debug &

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
