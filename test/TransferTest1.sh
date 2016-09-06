oldfile=/opt/HiDRA/test_file.cbf
newfile=/opt/HiDRA/data/source/local/test_file.cbf
movedfile=/opt/HiDRA/data/target/local/test_file.cbf

procname=HiDRA_test

python /opt/HiDRA/src/sender/DataManager.py --configFile /opt/HiDRA/test/dataManager.conf --procname $procname --verbose &

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

killall $procname
