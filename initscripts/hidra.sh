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
PATH=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin
DESC="HiDRA"
# Process name (for display)
NAME=hidra
SCRIPT_PROC_NAME=hidra
BASEDIR=/opt/hidra
DAEMON=$BASEDIR/src/sender/datamanager.py
DAEMON_EXE=$BASEDIR/datamanager
DAEMON_ARGS="--verbose --procname $NAME"
CONFIG_PATH=$BASEDIR/conf
CONFIG_FILE=$CONFIG_PATH/datamanager.conf
DAEMON_EXE_ARGS="$DAEMON_ARGS --config_file $CONFIG_FILE --procname $NAME"
PIDFILE_LOCATION=/opt/hidra
PIDFILE=${PIDFILE_LOCATION}/$NAME.pid
IPCPATH=/tmp/hidra
PYTHON=/usr/bin/python
LOG_DIRECTORY=/var/log/hidra

SCRIPTNAME=/etc/init.d/$NAME


if [ -f /etc/redhat-release -o -f /etc/centos-release ] ; then
# Red Hat or Centos...

    # source function library.
    . /etc/rc.d/init.d/functions

    BLUE=$(tput setaf 4)
    NORMAL=$(tput sgr0)
    GREEN=$(tput setaf 2)
    RED=$(tput setaf 1)

    if [ -n "$2" ] && [ -n "$3" ]
    then
        # set variables
        BEAMLINE="$2"
        DETECTOR="$3"
        NAME=${SCRIPT_PROC_NAME}_${BEAMLINE}_${DETECTOR}
        DAEMON_ARGS="--verbose --config_file ${CONFIG_PATH}/datamanager_${BEAMLINE}_${DETECTOR}.conf --procname $NAME"
        PIDFILE=${PIDFILE_LOCATION}/${NAME}.pid
    fi


    start()
    {
        status ${NAME} > /dev/null 2>&1 && status="1" || status="$?"
        # If the status is RUNNING then don't need to start again.
        if [ $status = "1" ]; then
            printf "$NAME is already running\n"
            return 0
        fi

    	printf "%-50s" "Starting ${NAME}..."
	    ${DAEMON} ${DAEMON_ARGS} &
    	RETVAL=$?

        TIMEOUT=0
        status ${NAME} > /dev/null 2>&1 && status="1" || status="$?"
        while [ $status != "1" ] && [ $TIMEOUT -lt 5 ] ; do
            sleep 1
            let TIMEOUT=TIMEOUT+1
            status ${NAME} > /dev/null 2>&1 && status="1" || status="$?"
        done

        status ${NAME} > /dev/null 2>&1 && status="1" || status="$?"
        if [ $status = "1" ]; then
            printf "%4s\n" "[ ${GREEN}OK${NORMAL} ]"
            return 0
        else
            printf "%4s\n" "[ ${RED}FAILED${NORMAL} ]"
            return $RETVAL
        fi
        echo
    }

    stop()
    {
        #check_status_q || exit 0
        status ${NAME} > /dev/null 2>&1 && status="1" || status="$?"
        # If the status is not RUNNING then don't need to stop again.
        if [ $status != "1" ]; then
            printf "$NAME is already stopped\n"
            return 0
        fi

    	printf "%-50s" "Stopping ${HIDRA}..."
        HIDRA_PID="`pidofproc ${NAME}`"
        # stop gracefully and wait up to 180 seconds.
        kill $HIDRA_PID > /dev/null 2>&1

        TIMEOUT=0
#        while checkpid $HIDRA_PID && [ $TIMEOUT -lt 30 ] ; do
        while checkpid $HIDRA_PID && [ $TIMEOUT -lt 5 ] ; do
            sleep 1
            let TIMEOUT=TIMEOUT+1
        done

        if checkpid $HIDRA_PID ; then
            killall -KILL $NAME

            SOCKETID=`cat $PIDFILE`

            rm -f "${IPCPATH}/${SOCKETID}"*
        fi

        status ${NAME} > /dev/null 2>&1 && status="1" || status="$?"
        if [ $status != "1" ]; then
            printf "%4s\n" "[ ${GREEN}OK${NORMAL} ]"
            return 0
        else
            printf "%4s\n" "[ ${RED}FAILED${NORMAL} ]"
            return $RETVAL
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
            printf "Restarting ${NAME}: \n"
            stop
            start
            ;;
        status)
            status ${NAME}
            RETVAL=$?
            ;;
        *)
            printf "Usage: $0 {start|stop|status|restart}\n"
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

    if [ -n "$2" ] && [ -n "$3" ]
    then
        # set variables
        BEAMLINE="$2"
        DETECTOR="$3"
        NAME=${SCRIPT_PROC_NAME}_${BEAMLINE}_${DETECTOR}
        DAEMON_ARGS="--verbose --config_file ${CONFIG_PATH}/datamanager_${BEAMLINE}_${DETECTOR}.conf --procname $NAME"
        PIDFILE=${PIDFILE_LOCATION}/${NAME}.pid
    fi

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
                exit 0
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

elif [ -f /etc/SuSE-release ] ; then
# SuSE

# source: /etc/init.d/skeleton

    # source function library.
#    . /lib/lsb/init-functions

    # Shell functions sourced from /etc/rc.status
    . /etc/rc.status


    # Reset status of this service
    rc_reset

    cleanup()
    {
        SOCKETID=`cat $PIDFILE`
        /bin/rm -rf "${IPCPATH}/${SOCKETID}"*

        # Many daemons don't delete their pidfiles when they exit.
        /bin/rm -rf $PIDFILE
        return 0

    }

    do_start()
    {
        printf "Starting $NAME"
        export LD_LIBRARY_PATH=/opt/hidra:$LD_LIBRARY_PATH

        # Checking if the process is already running
        /sbin/checkproc $NAME > /dev/null && status="0" || status="$?"
        # 0: service is up and running
        if [ $status = "0" ]; then
           printf "\n$NAME is already running"
        else
            # Create the directory for the log files
            if [ ! -d "$LOG_DIRECTORY" ]; then
                mkdir $LOG_DIRECTORY
                chmod 1777 $LOG_DIRECTORY
            fi

            ## Start daemon with startproc(8). If this fails
            ## the return value is set appropriately by startproc.
            /sbin/startproc $DAEMON_EXE $DAEMON_EXE_ARGS

            sleep 5

            /sbin/checkproc $NAME
        fi

        # Remember status and be verbose
        rc_status -v
    }

    do_stop()
    {
        printf "Stopping $NAME"

        # Checking if the process is running at all
        /sbin/checkproc $NAME > /dev/null && status="0" || status="$?"
        # 3: service is not running
        if [ $status = "3" ]; then
           printf "\n$NAME is not running"
        fi

        ## Stop daemon with killproc(8) and if this fails
        ## killproc sets the return value according to LSB
        /sbin/killproc -TERM $NAME

        # Remember status and be verbose
        rc_status -v
    }

    case "$1" in
        start)
            do_start
            ;;
        stop)
            do_stop
            ;;
        status)
            printf "Checking for service $NAME "
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
            /sbin/checkproc $NAME
            # NOTE: rc_status knows that we called this init script with
            # "status" option and adapts its messages accordingly.
            rc_status -v
            ;;
        #reload|force-reload)
            # If do_reload() is not implemented then leave this commented out
            # and leave 'force-reload' as an alias for 'restart'.

            #log_daemon_msg "Reloading $DESC" "$NAME"
            #do_reload
            #log_end_msg $?
            #;;
        restart|force-reload)
            printf "Not implemented yet\n"
            do_stop
            do_start
            ;;
        getsettings)
            $BASEDIR/getsettings
            ;;
        *)
            #echo "Usage: $SCRIPTNAME {start|stop|restart|reload|force-reload}" >&2
            printf "Usage: $SCRIPTNAME {start|stop|status|restart|force-reload|getsettings}\n" >&2
            exit 3
            ;;
    esac

fi
