#!/bin/bash

# LSB compliant init-script header.
### BEGIN INIT INFO
# Provides:          HiDRA
# Required-Start:    $syslog networking
# Required-Stop:     $syslog networking
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start the HiDRA
### END INIT INFO


# DIR should only include /usr/* if it runs after the mountnfs.sh script
DIR=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin
DESC="HiDRA"
# Process name (for display)
NAME=hidra
SCRIPT_PROC_NAME=hidra
IPCDIR=/tmp/hidra
PYTHON=/usr/bin/python
CURRENTDIR="$(readlink --canonicalize-existing -- "$0")"

USE_EXE=false

if [ "${USE_EXE}" == "false" ]
then
    BASEDIR=/home/kuhnm/projects/hidra
    #BASEDIR=/opt/hidra
    SCRIPTNAME=/etc/init.d/$NAME
else
    BASEDIR="${CURRENTDIR%/*}"
    SCRIPTNAME="${CURRENTDIR##*/}"

#    printf "CURRENTDIR: ${CURRENTDIR}\n"
#    printf "BASEDIR: ${BASEDIR}\n"
#    printf "SCRIPTNAME: ${SCRIPTNAME}\n"
fi

PIDFILE_LOCATION=${BASEDIR}
PIDFILE=${PIDFILE_LOCATION}/$NAME.pid
CONFIGDIR=$BASEDIR/conf

usage()
{
    printf "Usage: $SCRIPTNAME --beamline|--bl <beamline> [--detector|--det <detector>] [--config_file <config_file>] {--start|--stop|--status|--restart|--getsettings}\n" >&2
    RETVAL=3
}


beamline=
detector=
config_file=
action=
while test $# -gt 0
do
    case $1 in
        --bl | --beamline)
            beamline=$2
            shift
            ;;
        --det | --detector)
            detector=$2
            shift
            ;;
        --config_file)
            config_file=$2
            shift
            ;;
        --start)
            action="start"
            printf "set action to start\n"
            shift
            ;;
        --status)
            action="status"
            shift
            ;;
        --stop)
            action="stop"
            shift
            ;;
        --restart)
            action="restart"
            shift
            ;;
        --getsettings)
            action="getsettings"
            shift
            ;;
        -h | --help ) usage
            exit
            ;;
        * ) break;  # end of options
    esac
    shift
done

if [ -z ${action+x} ]
then
    usage
    exit 1
fi

if [ -n "$beamline" ]
then
    if [ -n "${detector}" ]
    then
        NAME=${SCRIPT_PROC_NAME}_${beamline}_${detector}
        config_file=${CONFIGDIR}/datamanager_${beamline}_${detector}.conf
    else
        NAME=${SCRIPT_PROC_NAME}_${beamline}
        config_file=${CONFIGDIR}/datamanager_${beamline}.conf
    fi
    PIDFILE=${PIDFILE_LOCATION}/${NAME}.pid
else
    printf "No beamline or detector specified. Fallback to default configuration file"
    config_file=$CONFIGDIR/datamanager.conf
fi

if [ "${USE_EXE}" == "false" ]
then
    DAEMON=${BASEDIR}/src/sender/datamanager.py
    DAEMON_ARGS="--verbose --procname ${NAME} --config_file ${config_file}"
    LOG_DIRECTORY=/var/log/hidra
    getsettings=${BASEDIR}/src/shared/getsettings.py
else
    DAEMON=${BASEDIR}/datamanager
    DAEMON_ARGS="--verbose --procname ${NAME} --config_file ${config_file}"
    LOG_DIRECTORY=${BASEDIR}/logs
    getsettings=${BASEDIR}/getsettings
fi

printf "\nSetting: \n"
printf "DAEMON=${DAEMON}\n"
printf "DAEMON_ARGS=${DAEMON_ARGS}\n"
printf "NAME=${NAME}\n"
printf "config_file=${config_file}\n\n"

if [ -f /etc/redhat-release -o -f /etc/centos-release ] ; then
# Red Hat or Centos...

    # source function library.
    . /etc/rc.d/init.d/functions

    BLUE=$(tput setaf 4)
    NORMAL=$(tput sgr0)
    GREEN=$(tput setaf 2)
    RED=$(tput setaf 1)

    do_start()
    {
        status ${NAME} > /dev/null 2>&1 && status="1" || status="$?"
        # If the status is RUNNING then don't need to start again.
        if [ $status = "1" ]; then
            printf "$NAME is already running\n"
            return 0
        fi

    	printf "%-50s" "Starting ${NAME}..."
        export LD_LIBRARY_PATH=${BASEDIR}:$LD_LIBRARY_PATH

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

    do_stop()
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

            rm -f "${IPCDIR}/${SOCKETID}"*
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

    do_status()
    {
        status ${NAME}
        RETVAL=$?
    }

    do_restart()
    {
        printf "Restarting ${NAME}: \n"
        do_stop
        do_start
    }

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

    #
    # Function that starts the daemon/service
    #
    do_start()
    {
        log_daemon_msg "Starting $NAME"
        export LD_LIBRARY_PATH=${BASEDIR}:$LD_LIBRARY_PATH

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

        case "$?" in
            0) log_end_msg 0
                ;;
            *) log_end_msg 1
                ;;
        esac
    }

    cleanup()
    {
        SOCKETID=`cat $PIDFILE`
        /bin/rm -rf "${IPCDIR}/${SOCKETID}"*

        # Many daemons don't delete their pidfiles when they exit.
        /bin/rm -rf $PIDFILE
        return 0

    }

    #
    # Function that stops the daemon/service
    #
    do_stop()
    {
        log_daemon_msg "Stopping $NAME"

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

        case "$?" in
            0) log_end_msg 0
                ;;
            *) log_end_msg 1
                ;;
        esac
    }

    do_status()
    {
        status_of_proc $NAME $NAME && exit 0 || exit $?
    }

    #
    # Function that sends a SIGHUP to the daemon/service
    #
    do_reload()
    {
        log_daemon_msg "Reloading $DESC" "$NAME"

        # If the daemon can reload its configuration without
        # restarting (for example, when it is sent a SIGHUP),
        # then implement that here.
        start-stop-daemon --stop --signal 1 --quiet --pidfile $PIDFILE --name $NAME
        log_end_msg 0
    }

    do_restart()
    {
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
    }

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
        /bin/rm -rf "${IPCDIR}/${SOCKETID}"*

        # Many daemons don't delete their pidfiles when they exit.
        /bin/rm -rf $PIDFILE
        return 0

    }

    do_start()
    {
        printf "Starting $NAME"
        export LD_LIBRARY_PATH=${BASEDIR}:$LD_LIBRARY_PATH

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
            /sbin/startproc $DAEMON $DAEMON_ARGS

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

    do_status()
    {
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
    }

    do_restart()
    {
        printf "Not tested yet\n"
        do_stop
        do_start
    }

fi

case "$action" in
    start)
        do_start
        ;;
    stop)
        do_stop
        ;;
    status)
        do_status
        ;;
    #reload|force-reload)
        # If do_reload() is not implemented then leave this commented out
        # and leave 'force-reload' as an alias for 'restart'.

        #log_daemon_msg "Reloading $DESC" "$NAME"
        #do_reload
        #log_end_msg $?
        #;;
    #restart|force-reload)
        # If the "reload" option is implemented then remove the
        # 'force-reload' alias
    restart)
        do_restart
        ;;
    getsettings)
        ${getsettings} --config_file ${config_file}
        ;;
    *)
        usage
        ;;
esac
exit $RETVAL
