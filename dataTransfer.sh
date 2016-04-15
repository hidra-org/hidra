#!/bin/bash
INSTANZ="DataManager"
Pidfile=/root/zeromq-data-transfer/DataManager.pid

DATAMANAGER=/root/zeromq-data-transfer/src/sender/DataManager.py

if [ -f $Pidfile ]
then
	Pid=`cat $Pidfile`
fi


start() {
		if [ -f $Pidfile ] ; then
				if test `ps -e | grep -c $Pid` = 1; then
						echo "Not starting $INSTANZ - instance already running"
#						echo "Not starting $INSTANZ - instance already running with PID: $Pid"
				else
						echo "Starting $INSTANZ"
						nohup /usr/bin/python ${DATAMANAGER} &> /dev/null &
                        $(ps -ef | grep '${DATAMANAGER}' | awk '{ print $2 }') > $PIDfile
				fi
		else
				echo "Starting $INSTANZ"
				nohup /usr/bin/python ${DATAMANAGER} &> /dev/null &
				echo $! > $Pidfile
		fi
}


stop()
{
		if [ -f $Pidfile ] ; then
				echo "Stopping $INSTANZ"
                pkill python
#                echo ${pid}
#                kill -15 $Pid
                rm $Pidfile
#                echo $!
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
#						echo "$INSTANZ running with PID: [$Pid]"
						echo "$INSTANZ running"
				fi
		else
				echo "$Pidfile does not exist! Cannot process $INSTANZ status!"
				exit 1
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



