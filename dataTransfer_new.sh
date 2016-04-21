#!/bin/sh

# LSB compliant init-script header.
### BEGIN INIT INFO
# Provides:          zeromq-data-transfer
# Required-Start:    $syslog networking
# Required-Stop:     $syslog networking
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start the ZMQ data transfer
### END INIT INFO


DESC="ZMQ data transfer"
BASE_DIR=/space/projects/zeromq-data-transfer
PID_DIR=/space/projects/zeromq-data-transfer
PID_FILE=${PID_DIR}/zeromq-data-transfer.pid
DATAMANAGER=${BASE_DIR}/src/sender/DataManager.py
DATAMANAGER_ARGS="--verbose"
# Process name ( For display )
NAME=zeromq-data-transfer


if [ -f /etc/redhat-release -o -f /etc/centos-release ] ; then
# Red Hat or Centos...

    # source function library.
    . /etc/rc.d/init.d/functions

    RETVAL=0
    start()
    {
    	echo -n "Starting ${DESC}..."
	    /usr/bin/python ${DATAMANAGER} --pidfile ${PID_FILE}
    	RETVAL=$?
	    [ "$RETVAL" = 0 ] && touch /var/lock/subsys/zeromq-data-transfer
    	echo
    }

    stop()
    {
	    echo -n "Stopping ${DESC}..."
    	if [ -n "`cat ${PID_FILE} 2>/dev/null`" ];
	    then
	        DATATRANSFER_PID="`pidofproc -p ${PID_FILE} ${NAME}`"
    	    if [ -z "$DATATRANSFER_PID" ]; then
        		exit 0
	        fi
     	    # stop gracefully and wait up to 180 seconds.
	        kill -TERM $DATATRANSFER_PID >/dev/null 2>&1
	        TIMEOUT=0
    	    while checkpid $DATATRANSFER_PID && [ $TIMEOUT -lt 180 ] ; do
                sleep 1
                let TIMEOUT=TIMEOUT+1
            done
            if checkpid $DATATRANSFER_PID ; then
                #TODO kill subprocesses too
                kill -KILL $DATATRANSFER_PID
            fi
	    fi
    	RETVAL=$?
    	[ "$RETVAL" = 0 ] && rm -f /var/lock/subsys/zeromq-data-transfer
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
            status -p ${PID_FILE} ${NAME}
            RETVAL=$?
            ;;
        *)
            echo "Usage: $0 {start|stop|restart}"
            RETVAL=1
            ;;
    esac
    exit $RETVAL

elif [ -f /etc/SuSE-release ] ; then
# Suse

    # check whether executable is available.
    test -x ${DATAMANAGER} || exit 5

# Shell functions sourced from /etc/rc.status:
#      rc_check         check and set local and overall rc status
#      rc_status        check and set local and overall rc status
#      rc_status -v     be verbose in local rc status and clear it afterwards
#      rc_status -v -r  ditto and clear both the local and overall rc status
#      rc_status -s     display "skipped" and exit with status 3
#      rc_status -u     display "unused" and exit with status 3
#      rc_failed        set local and overall rc status to failed
#      rc_failed <num>  set local and overall rc status to <num>
#      rc_reset         clear both the local and overall rc status
#      rc_exit          exit appropriate to overall rc status
#      rc_active        checks whether a service is activated by symlinks
    . /etc/rc.status

# Reset status of this service
    rc_reset

# Return values acc. to LSB for all commands but status:
# 0	  - success
# 1       - generic or unspecified error
# 2       - invalid or excess argument(s)
# 3       - unimplemented feature (e.g. "reload")
# 4       - user had insufficient privileges
# 5       - program is not installed
# 6       - program is not configured
# 7       - program is not running
# 8--199  - reserved (8--99 LSB, 100--149 distrib, 150--199 appl)
#
# Note that starting an already running service, stopping
# or restarting a not-running service as well as the restart
# with force-reload (in case signaling is not supported) are
# considered a success.

    case "$1" in
        start)
            echo -n "Starting ${DESC}..."
            ## Start daemon with startproc(8). If this fails
            ## the return value is set appropriately by startproc.
            /sbin/startproc -v ${DATAMANAGER} --pidfile ${PID_FILE}

            # Remember status and be verbose
            rc_status -v
            ;;
        stop)
            echo -n "Stopping ${DESC}..."
            ## Stop daemon with killproc(8) and if this fails
            ## killproc sets the return value according to LSB.
            /sbin/killproc -v -t 60 -p ${PID_FILE} -TERM ${DATAMANAGER}

            # Remember status and be verbose
            rc_status -v
            ;;
        try-restart|condrestart)
            ## Do a restart only if the service was active before.
            ## Note: try-restart is now part of LSB (as of 1.9).
            ## RH has a similar command named condrestart.
            if test "$1" = "condrestart"; then
                echo "${attn} Use try-restart ${done}(LSB)${attn} rather than condrestart ${warn}(RH)${norm}"
            fi
            $0 status
            if test $? = 0; then
                $0 restart
            else
                rc_reset	# Not running is not a failure.
            fi
            # Remember status and be quiet
            rc_status
            ;;
        restart)
            ## Stop the service and regardless of whether it was
            ## running or not, start it again.
            $0 stop
            $0 start

            # Remember status and be quiet
            rc_status
            ;;
        force-reload)
            ## Signal the daemon to reload its config. Most daemons
            ## do this on signal 1 (SIGHUP).
            ## If it does not support it, restart the service if it
            ## is running.

            echo -n "Force-Reloading ${DESC}..."

            ## Otherwise:
            $0 try-restart
            rc_status
            ;;
        reload)
            ## Like force-reload, but if daemon does not support
            ## signaling, do nothing (!)

            # If it supports signaling:
            echo -n "${DESC} doesn't support reload."
            #/sbin/killproc -HUP $FOO_BIN
            #touch /var/run/FOO.pid
            #rc_status -v

            ## Otherwise if it does not support reload:
            #rc_failed 3
            #rc_status -v
            ;;
        status)
            echo -n "Checking for service ${NAME}: "
            ## Check status with checkproc(8), if process is running
            ## checkproc will return with exit status 0.

            # Return value is slightly different for the status command:
            # 0 - service up and running
            # 1 - service dead, but /var/run/  pid  file exists
            # 2 - service dead, but /var/lock/ lock file exists
            # 3 - service not running (unused)
            # 4 - service status unknown :-(
            # 5--199 reserved (5--99 LSB, 100--149 distro, 150--199 appl.)

            # NOTE: checkproc returns LSB compliant status values.
            /sbin/checkproc $DATAMANAGER
            # NOTE: rc_status knows that we called this init script with
            # "status" option and adapts its messages accordingly.
            rc_status -v
            ;;
        *)
            echo "Usage: $0 {start|stop|status|try-restart|restart|force-reload|reload}"
            exit 1
            ;;
    esac
    rc_exit

elif [ -f /etc/debian_version ] ; then
# Debian and Ubuntu

    # PATH should only include /usr/* if it runs after the mountnfs.sh script
    PATH=/sbin:/usr/sbin:/bin:/usr/bin
    SCRIPTNAME=/etc/init.d/$NAME

## Exit if the package is not installed
#    [ -x "$DAEMON" ] || exit 0

# Load the VERBOSE setting and other rcS variables
#    . /lib/init/vars.sh

# Define LSB log_* functions.
# Depend on lsb-base (>= 3.2-14) to ensure that this file is present
# and status_of_proc is working.
    . /lib/lsb/init-functions

#
# Function that starts the daemon/service
#
    do_start()
    {
        # Return
        #   0 if daemon has been started
        #   1 if daemon was already running
        #   2 if daemon could not be started
        start-stop-daemon --start --quiet --pidfile $PID_FILE --make-pidfile --exec $DATAMANAGER --test > /dev/null \
            || return 1

        start-stop-daemon --start --quiet --pidfile $PID_FILE --make-pidfile \
            --exec /usr/bin/python $DATAMANAGER -- $DATAMANAGER_ARGS & 1> /dev/null \
            || return 2

    }

#
# Function that stops the daemon/service
#
    do_stop()
    {
        # Return
        #   0 if daemon has been stopped
        #   1 if daemon was already stopped
        #   2 if daemon could not be stopped
        #   other if a failure occurred
        start-stop-daemon --stop --quiet --pidfile $PID_FILE
#        start-stop-daemon --stop --quiet --retry=TERM/180/KILL/5 --pidfile $PID_FILE

        RETVAL="$?"
        [ "$RETVAL" = 2 ] && return 2
        # Wait for children to finish too if this is a daemon that forks
        # and if the daemon is only ever run from this initscript.
        # If the above conditions are not satisfied then add some other code
        # that waits for the process to drop all resources that could be
        # needed by services started subsequently.  A last resort is to
        # sleep for some time.
        start-stop-daemon --stop --quiet --oknodo --retry=0/180/KILL/5 --exec $DATAMANAGER
        [ "$?" = 2 ] && return 2
        # Many daemons don't delete their pidfiles when they exit.
        rm -f $PID_FILE
        return "$RETVAL"
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
            [ "$VERBOSE" != no ] && log_daemon_msg "Starting $DESC" "$NAME"
            do_start
            case "$?" in
                0|1) [ "$VERBOSE" != no ] && log_end_msg 0
                    ;;
                2) [ "$VERBOSE" != no ] && log_end_msg 1
                    ;;
            esac
            ;;
        stop)
            [ "$VERBOSE" != no ] && log_daemon_msg "Stopping $DESC" "$NAME"
            do_stop
            case "$?" in
                0|1) [ "$VERBOSE" != no ] && log_end_msg 0
                    ;;
                2) [ "$VERBOSE" != no ] && log_end_msg 1
                    ;;
            esac
            ;;
        status)
            status_of_proc "$DATAMANAGER" "$NAME" && exit 0 || exit $?
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
            do_stop
            case "$?" in
                0|1)
                    do_start
                    case "$?" in
                        0) log_end_msg 0
                            ;;
                        1) log_end_msg 1 # Old process is still running
                            ;;
                        *) log_end_msg 1 # Failed to start
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
