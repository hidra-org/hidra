#!/bin/bash
INSTANZ="DataManager"
Pidfile=/space/projects/zeromq-data-transfer/DataManager.pid
#Pidfile=/root/zeromq-data-transfer/DataManager.pid

DATAMANAGER=/space/projects/zeromq-data-transfer/src/sender/DataManager.py
#DATAMANAGER=/root/zeromq-data-transfer/src/sender/DataManager.py

if [ -f $Pidfile ]
then
	Pid=`cat $Pidfile`
fi


start()
{
    if [ -f $Pidfile ] && test `ps -e | grep -c $Pid` = 1; then
        echo "Not starting $INSTANZ - instance already running"
#        echo "Not starting $INSTANZ - instance already running with PID: $Pid"
    else
        echo "Starting $INSTANZ"
        nohup /usr/bin/python ${DATAMANAGER} &
        echo $! > $Pidfile
    fi
}


stop()
{
    if [ -f $Pidfile ] ; then
        echo "Stopping $INSTANZ"
#        pkill python
        kill -15 $Pid
        rm $Pidfile
    else
        echo "Cannot stop $INSTANZ - no Pidfile found!"
    fi
}

status()
{
    if [ -f $Pidfile ] ; then
        if test `ps -e | grep -c $Pid` = 0; then
            echo "$INSTANZ not running"
        else
#            echo "$INSTANZ running with PID: [$Pid]"
            echo "$INSTANZ running"
        fi
    else
        echo "$INSTANZ not running"
#        echo "$Pidfile does not exist! Cannot process $INSTANZ status!"
#        exit 1
    fi
}


case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        sleep 5
        start
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 {start | stop | restart | status}"
        exit 1
esac
exit 0



