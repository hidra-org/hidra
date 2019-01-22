# tests file moving on basis of inotifyx while OnDA is asking to get the files as well

SCRIPTDIR=$(readlink -f $0)
BASEDIR=${SCRIPTDIR%%/test/TransferTest4.sh}

oldfile=$BASEDIR/test/test_files/test_file.cbf
newfile=$BASEDIR/data/source/local/test_file.cbf
movedfile=$BASEDIR/data/target/local/test_file.cbf

procname=HiDRA_test4
onda_procname=example_onda


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

python $BASEDIR/test/API/example_onda.py \
    --procname $onda_procname &

sleep 1

cp $oldfile $newfile
echo "Copy done"

sleep 2

numberOfErrors=$(less $BASEDIR/logs/${procname}.log* | grep "ERROR" | wc -l)
exitstatus=0

if [ $numberOfErrors != 0 ]; then
    echo "Errors in transfer"
    less $BASEDIR/logs/${procname}.log* | grep "ERROR"
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
rm $BASEDIR/logs/${procname}.log*

sleep 1

if [ $exitstatus != 0 ]; then
    exit $exitstatus
fi
