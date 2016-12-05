#!/bin/sh

# LSB compliant init-script header.
### BEGIN INIT INFO
# Provides:          HiDRA
# Required-Start:    $syslog networking
# Required-Stop:     $syslog networking
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start the ZMQ data transfer
### END INIT INFO


# PATH should only include /usr/* if it runs after the mountnfs.sh script
PATH=/sbin:/usr/sbin:/bin:/usr/bin
DESC="HiDRA"
# Process name ( For display )
NAME=hidra
DAEMON=/opt/hidra/src/sender/DataManager.py
DAEMON_ARGS="--verbose"
PIDFILE=/opt/hidra/$NAME.pid
IPCPATH=/tmp/hidra
PYTHON=/usr/bin/python

SCRIPTNAME=/etc/init.d/$NAME


if [ -f /etc/redhat-release -o -f /etc/centos-release ] ; then
# Red Hat or Centos...

    # source function library.
    . /etc/rc.d/init.d/functions

    start()
    {
    	echo -n "Starting ${DESC}..."
	    ${DAEMON} ${DAEMON_ARGS} &
        echo $! > $PIDFILE
    	RETVAL=$?
#	    [ "$RETVAL" = 0 ] && touch /var/lock/subsys/hidra
    	echo
    }

    stop()
    {
	    echo -n "Stopping ${DESC}..."
        DATATRANSFER_PID="`pidofproc ${NAME}`"
        # stop gracefully and wait up to 180 seconds.
#        if [ -z "$DATATRANSFER_PID" ]; then
#            exit 0
#        else
            kill $DATATRANSFER_PID > /dev/null 2>&1
#        fi

        TIMEOUT=0
#        while checkpid $DATATRANSFER_PID && [ $TIMEOUT -lt 30 ] ; do
        while checkpid $DATATRANSFER_PID && [ $TIMEOUT -lt 5 ] ; do
            sleep 1
            let TIMEOUT=TIMEOUT+1
        done
        echo $DATATRANSFER_PID

        if checkpid $DATATRANSFER_PID ; then
            killall -KILL $NAME

            SOCKETID=`cat $PIDFILE`

            rm -f "${IPCPATH}/${SOCKETID}"*
        fi
        rm -f $PIDFILE
    	RETVAL=$?
#    	[ "$RETVAL" = 0 ] && rm -f /var/lock/subsys/hidra
	    echo
    }

    case "$1" in
        start)
            start
            ;;
        stop)
            stop
            ;;
        restart)
            echo -n "Restarting ${DESC}: "
            stop
            start
            ;;
        status)
            status ${NAME}
            RETVAL=$?
            ;;
        *)
            echo "Usage: $0 {start|stop|status|restart}"
            RETVAL=1
            ;;
    esac
    exit $RETVAL

elif [ -f /etc/debian_version ] ; then
# Debian and Ubuntu

    # Exit if the package is not installed
#    [ -x "$DAEMON" ] || exit 0

    # Load the VERBOSE setting and other rcS variables
#    . /lib/init/vars.sh

    # Define LSB log_* functions.
    # Depend on lsb-base (>= 3.2-14) to ensure that this file is present
    # and status_of_proc is working.
    . /lib/lsb/init-functions

#    ln -s "$DAEMON" "$NAME"

    #
    # Function that starts the daemon/service
    #
    do_start()
    {
        # Checked the PID file exists and check the actual status of process
        if [ -e $PIDFILE ]; then
            status_of_proc -p $PIDFILE $DAEMON "$NAME" > /dev/null && status="1" || status="$?"
            # If the status is SUCCESS then don't need to start again.
            if [ $status = "1" ]; then
                log_daemon_msg "$NAME is already running"
                return 0
                exit
            fi
        fi

        # Start the daemon with the help of start-stop-daemon
        # possible exit status
        # 0      The requested action was performed. If --oknodo was specified,
        #        it's also possible that nothing had to be done.  This can
        #        happen when --start was specified and a matching process was
        #        already running, or when --stop was specified and there were
        #        no matching processes.
        # 1      If --oknodo was not specified and nothing was done.
        # 2      If --stop and --retry were specified, but the end of the
        #        schedule was reached and the processes were still running.
        # 3      Any other error.
        # (--oknodo: If the a process exists start-stop-daemon exits with error
        #  status 0 instead of 1)
        if start-stop-daemon --start --quiet --pidfile $PIDFILE --make-pidfile --background \
            --startas $DAEMON -- $DAEMON_ARGS ; then
            return 0
        else
            return 1
        fi
    }

    cleanup()
    {
        SOCKETID=`cat $PIDFILE`
        /bin/rm -rf "${IPCPATH}/${SOCKETID}"*

        # Many daemons don't delete their pidfiles when they exit.
        /bin/rm -rf $PIDFILE
        return 0

    }

    #
    # Function that stops the daemon/service
    #
    do_stop()
    {
        # Stop the daemon.
        if [ -e $PIDFILE ]; then
#            status_of_proc $NAME $NAME && exit 0 || exit $?
            status_of_proc $NAME "$NAME" > /dev/null && status="0" || status="$?"
            if [ "$status" = 0 ]; then
                start-stop-daemon --stop --quiet --pidfile $PIDFILE #--name $NAME
#                start-stop-daemon --stop --quiet --retry=TERM/180/KILL/5 --pidfile $PIDFILE
                daemon_status="$?"
                if [ "$daemon_status" = 2 ]; then
                    cleanup
                    return 1
                elif [ "$daemon_status" = 0 ]; then
                    cleanup
                    return 0
                fi

                # Wait for children to finish too if this is a daemon that forks
                # and if the daemon is only ever run from this initscript.
                # If the above conditions are not satisfied then add some other code
                # that waits for the process to drop all resources that could be
                # needed by services started subsequently.  A last resort is to
                # sleep for some time.
                start-stop-daemon --stop --quiet --oknodo --retry=0/30/KILL/5 --exec $DAEMON
                [ "$?" = 2 ] && cleanup && return 1

                cleanup
            else
                cleanup
            fi
        else
            log_daemon_msg "$NAME is not running"
        fi
    }

    #
    # Function that sends a SIGHUP to the daemon/service
    #
    do_reload()
    {
        # If the daemon can reload its configuration without
        # restarting (for example, when it is sent a SIGHUP),
        # then implement that here.
        start-stop-daemon --stop --signal 1 --quiet --pidfile $PIDFILE --name $NAME
        return 0
    }

    case "$1" in
        start)
            log_daemon_msg "Starting $NAME"
            do_start
            case "$?" in
                0) log_end_msg 0
                    ;;
                *) log_end_msg 1
                    ;;
            esac
            ;;
        stop)
            log_daemon_msg "Stopping $NAME"
            do_stop
            case "$?" in
                0) log_end_msg 0
                    ;;
                *) log_end_msg 1
                    ;;
            esac
            ;;
        status)
            status_of_proc $NAME $NAME && exit 0 || exit $?
            ;;
        #reload|force-reload)
            # If do_reload() is not implemented then leave this commented out
            # and leave 'force-reload' as an alias for 'restart'.

            #log_daemon_msg "Reloading $DESC" "$NAME"
            #do_reload
            #log_end_msg $?
            #;;
        restart|force-reload)
            # If the "reload" option is implemented then remove the
            # 'force-reload' alias

            log_daemon_msg "Restarting $DESC" "$NAME"
            log_daemon_msg "Stopping $DESC" "$NAME"
            do_stop
            stop_status="$?"
            case "$stop_status" in
                0) log_end_msg 0
                    ;;
                *) log_end_msg 1
                    ;;
            esac
            sleep 3
            case "$stop_status" in
                0)
                    log_daemon_msg "Starting $NAME"
                    do_start
                    case "$?" in
                        0) log_end_msg 0
                            ;;
                        *) log_end_msg 1
                            ;;
                    esac
                    ;;
                *)
                    # Failed to stop
                    log_end_msg 1
                    ;;
            esac
            ;;
        *)
            #echo "Usage: $SCRIPTNAME {start|stop|restart|reload|force-reload}" >&2
            echo "Usage: $SCRIPTNAME {start|stop|status|restart|force-reload}" >&2
            exit 3
            ;;
    esac
fi