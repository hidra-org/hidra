# tests moving files gotten via HTTP

SCRIPTDIR=$(readlink -f "$0")
BASEDIR=${SCRIPTDIR%%/test/TransferTest3.sh}
echo "$BASEDIR"


oldfile=$BASEDIR/test/test_files/test_file.cbf
#newfile=$BASEDIR/data/source/local/test_file.cbf
movedfile=$BASEDIR/data/target/local/test_file.cbf

procname=HiDRA_test3


python "$BASEDIR/src/sender/datamanager.py" \
    --config_file "$BASEDIR/test/datamanager.yaml" \
    --procname "$procname" \
    --log_path "$BASEDIR/logs" \
    --log_name "${procname}.log" \
    --monitored_dir "$BASEDIR/data/source" \
    --local_target "$BASEDIR/data/target/local" \
    --ext_ip 0.0.0.0 \
    --eventdetector_type http_events \
    --datafetcher_type http_fetcher \
    --det_ip 131.169.55.170 \
    --det_api_version 1.5.0 \
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

rm "$movedfile"
rm "$BASEDIR/logs/${procname}.log"*

killall "$procname"

sleep 1
