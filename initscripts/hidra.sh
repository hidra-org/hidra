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
START_CHECK_DELAY=3

USE_EXE=false
SHOW_SCRIPT_SETTINGS=false
DEBUG=false

if [ "${USE_EXE}" == "false" ]
then
    BASEDIR=/opt/hidra
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

RETVAL=0

usage()
{
    #echo "$(basename "$0")"
    echo "Usage: $SCRIPTNAME COMMAND --beamline|--bl <beamline> [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "    start           starts hidra"
    echo "    stop            stops hidra"
    echo "    status          shows the status of hidra"
    echo "    restart         stops and restarts hidra"
    echo "    getsettings     if hidra is running, shows the configured settings"
    echo ""
    echo "Mandatory arguments:"
    echo "    --bl, --beamline        the beamline to start hidra for (mandatory,"
    echo "                            case insensitive)"
    echo ""
    echo "Options:"
    echo "    --det, --detector       the detector which is used"
    echo "    --config_file <string>  use a different config file"
    echo "    --debug                 enable verbose output for debugging purposes"
}

# for the environment to be set correctly these have to be loaded before the
# parameters are parsed
# e.g. SuSE: if this is done after ther parameters status results in failed
# and done instead of unused and running

if [ -f /etc/redhat-release -o -f /etc/centos-release ] ; then
# red hat or centos...

    # source function library.
    . /etc/rc.d/init.d/functions

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

elif [ -f /etc/SuSE-release ] ; then
# SuSE

# source: /etc/init.d/skeleton

    # source function library.
#    . /lib/lsb/init-functions

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
fi

beamline=
detector=
config_file=
action=
while test $# -gt 0
do
    #convert to lower case
    input_value=$(echo "$1" | tr '[:upper:]' '[:lower:]')

    case $input_value in
        start)
            action="start"
            ;;
        status)
            action="status"
            ;;
        stop)
            action="stop"
            ;;
        restart)
            action="restart"
            ;;
        getsettings)
            action="getsettings"
            ;;
        --bl | --beamline)
            # case insensitive
            beamline=$(echo "$2" | tr '[:upper:]' '[:lower:]')
            shift
            ;;
        --bl=* | --beamline=*)
            # case insensitive
            beamline=$(echo "${input_value#*=}" | tr '[:upper:]' '[:lower:]')
            shift
            ;;
        --det | --detector)
            detector=$2
            shift
            ;;
        --det=* | --detector=*)
            detector=${input_value#*=}
            shift
            ;;
        --config_file)
            config_file=$2
            shift
            ;;
        --config_file=*)
            config_file=${input_value#*=}
            shift
            ;;
        --debug)
            DEBUG=true
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

if [ ! -n "$config_file" ]
then
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
        printf "No beamline or detector specified. Fallback to default configuration file\n"
        config_file=$CONFIGDIR/datamanager.conf
    fi
fi

if [ "${USE_EXE}" == "false" ]
then
    DAEMON=${BASEDIR}/src/sender/datamanager.py
    DAEMON_ARGS="--verbose --procname ${NAME} --config_file ${config_file}"
    LOG_DIRECTORY=/var/log/hidra
    getsettings=${BASEDIR}/src/shared/getsettings.py
    get_receiver_status=${BASEDIR}/src/shared/get_receiver_status.py
else
    DAEMON=${BASEDIR}/datamanager
    DAEMON_ARGS="--verbose --procname ${NAME} --config_file ${config_file}"
    LOG_DIRECTORY=${BASEDIR}/logs
    getsettings=${BASEDIR}/getsettings
    get_receiver_status=${BASEDIR}/get_receiver_status
fi

if [ "${DEBUG}" == "true" ]
then
    DAEMON_ARGS="${DAEMON_ARGS} --onscreen debug"
    SHOW_SCRIPT_SETTINGS=true
fi

if [ "${SHOW_SCRIPT_SETTINGS}" == "true" ]
then
    printf "\nSetting: \n"
    printf "DAEMON=${DAEMON}\n"
    printf "DAEMON_ARGS=${DAEMON_ARGS}\n"
    printf "NAME=${NAME}\n"
    printf "config_file=${config_file}\n\n"
fi

if [ -f /etc/redhat-release -o -f /etc/centos-release ] ; then
# red hat or centos...

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

        # give it time to properly start up
        TIMEOUT=0
        status ${NAME} > /dev/null 2>&1 && status="1" || status="$?"
        while [ $status != "1" ] && [ $TIMEOUT -lt 5 ] ; do
            sleep 1
            let TIMEOUT=TIMEOUT+1
            status ${NAME} > /dev/null 2>&1 && status="1" || status="$?"
        done

        # detect status
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

        printf "%-50s" "Stopping ${NAME}..."
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

    #
    # Function that starts the daemon/service
    #
    do_start()
    {
        export LD_LIBRARY_PATH=${BASEDIR}:$LD_LIBRARY_PATH

        # Checked the PID file exists and check the actual status of process
        if [ -e $PIDFILE ]; then
            pidof $NAME >/dev/null
            status=$?
            # If the status is SUCCESS then don't need to start again.
            if [ $status -eq 0 ]; then
                log_success_msg "$NAME is already running"
                return 0
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
        if /sbin/start-stop-daemon --start --quiet --pidfile $PIDFILE --make-pidfile --background \
            --startas $DAEMON -- $DAEMON_ARGS ; then

            # give it time to properly start up
            sleep $START_CHECK_DELAY

            # detect status
            pidof $NAME >/dev/null
            status=$?
            if [ $status = "0" ]; then
                log_success_msg "Starting $NAME"
                return 0
            else
                log_failure_msg "Starting $NAME"
                return 1
            fi
        else
            log_failure_msg "Starting $NAME"
            return 1
        fi
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

        # Stop the daemon.
        if [ -e $PIDFILE ]; then
            log_msg="Stopping $NAME"

            # returns: 0 if process exists, 1 otherwise
            pidof $NAME > /dev/null
            status=$?
            # If the status is SUCCESS then don't need to start again.
            if [ $status -eq 0 ]; then
                /sbin/start-stop-daemon --stop --quiet --pidfile $PIDFILE #--name $NAME
#                /sbin/start-stop-daemon --stop --quiet --retry=TERM/180/KILL/5 --pidfile $PIDFILE

                daemon_status="$?"
                if [ "$daemon_status" = 2 ]; then
                    # stop successful but the end of the schedule was
                    # reached and the processes were still running
                    cleanup
                    log_success_msg $log_msg
                    return 1
                elif [ "$daemon_status" = 0 ]; then
                    # stop successful
                    cleanup
                    log_success_msg $log_msg
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
                log_success_msg $log_msg
            else
                cleanup
                log_success_msg $log_msg
            fi
        else
            log_success_msg "$NAME is not running"
        fi
    }

    do_status()
    {
        status_of_proc $NAME $NAME && return 0 || return $?
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
        do_stop
        sleep 3
        do_start
    }

elif [ -f /etc/SuSE-release ] ; then
# SuSE

    cleanup()
    {
        SOCKETID=`cat $PIDFILE`
        /bin/rm -rf "${IPCDIR}/${SOCKETID}"*

        # Many daemons don't delete their pidfiles when they exit.
        /bin/rm -rf $PIDFILE
        return 0

    }

    # rc_status can only work with return values
    # if the return value has to be checked by something else as well it has to
    # be reestablished for rc_status
    return_func()
    {
        return $1
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

            sleep $START_CHECK_DELAY

            /sbin/checkproc $NAME
            worked=$?

            running_procs=$(ps ax -o command --no-header | grep "hidra_" | grep -v grep | grep -v $NAME | sort -u)
            if [ "$running_procs" != "" ]
            then
                echo "Error when starting $NAME. Already running instances detected:"
                echo "$running_procs"
            fi

            return_func $worked
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

do_start_debug()
{
    printf "Starting $NAME"
    export LD_LIBRARY_PATH=${BASEDIR}:$LD_LIBRARY_PATH
    $DAEMON $DAEMON_ARGS
}

case "$action" in
    start)
        if [ "${DEBUG}" == "true" ]
        then
            do_start_debug
        else
            do_start
        fi
        ;;
    stop)
        do_stop
        ;;
    status)
        do_status
        echo
        export LD_LIBRARY_PATH=${BASEDIR}:$LD_LIBRARY_PATH
        ${get_receiver_status} --config_file ${config_file}
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
