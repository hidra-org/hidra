# tests file moving on basis of inotifyx

SCRIPTDIR=$(readlink -f $0)
BASEDIR=${SCRIPTDIR%%/test/TransferTest1.sh}

oldfile=$BASEDIR/test/test_files/test_file.cbf
newfile=$BASEDIR/data/source/local/test_file.cbf
movedfile=$BASEDIR/data/target/local/test_file.cbf

procname=HiDRA_test1


python $BASEDIR/src/sender/datamanager.py \
    --config_file $BASEDIR/test/datamanager.conf \
    --procname $procname \
    --log_path $BASEDIR/logs \
    --log_name ${procname}.log \
    --monitored_dir $BASEDIR/data/source \
    --local_target $BASEDIR/data/target \
    --ext_ip 0.0.0.0 \
    --eventdetector_type inotifyx_events \
    --datafetcher_type file_fetcher \
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
rm $BASEDIR/logs/${procname}.log*

killall $procname

sleep 1
