#!/usr/bin/env bash


#defaults values. will be overwritten by argument parsing if option has been set
SOURCE_DIR="/space/projects/Live_Viewer/source/"  # directory which will be watched for new files.
#SOURCE_DIR="/tmp/data"         # directory which will be watched for new files.
TARGET_IP="127.0.0.1"           # where new files will be send to via network
TARGET_PORT="6061"              # where new files will be send to via network
PARALLEL_DATA_STREAMS=1         # number of parallel streams used to send data to target
PYTHON_CMD=$(which python)      # executable of python
LOG_DIR="/space/projects/Live_Viewer/logs"         # place to put logfiles
#LOG_DIR="/tmp/log"              # place to put logfiles
FILE_MOVER_IP="127.0.0.1"       # ip-adress of fileMover process, it is waiting for new file events
FILE_MOVER_PORT="6060"          # port      of fileMover process, it is waiting for new file events
VERBOSE=0                       # set to 1 for more verbose output
PID_WATCHFOLDER=""
PID_FILEMOVER=""

startWatchFolder()
{
    echo "starting 'watcher_lsyncd.py'..."

    if [ "${VERBOSE}" == "0" ]
    then
        ${PYTHON_CMD} watcher_lsyncd.py --watchFolder="${SOURCE_DIR}" \
                              --logfilePath="${LOG_DIR}" \
                              --pushServerIp="${FILE_MOVER_IP}" \
                              --pushServerPort=${FILE_MOVER_PORT} \
                              --verbose 1>/dev/null 2>/dev/null &
    else
        ${PYTHON_CMD} watcher_lsyncd.py --watchFolder="${SOURCE_DIR}" \
                              --logfilePath="${LOG_DIR}" \
                              --pushServerIp="${FILE_MOVER_IP}" \
                              --pushServerPort=${FILE_MOVER_PORT} 1>/dev/null 2>/dev/null &
    fi
    local PID=${!}
    echo "starting 'watcher_lsyncd.py'... started as background job with PID=${PID}"
    PID_WATCHFOLDER=${PID}

    #to kill single process
    #kill -s 24 ${PID}

}


startFileMover()
{

    echo "starting 'fileMover.py'..."
    if [ "${VERBOSE}" == "0" ]
    then
        ${PYTHON_CMD} fileMover.py --logfilePath="${LOG_DIR}" \
                                   --bindingIpForSocket="127.0.0.1" \
                                   --logfileName="fileMover.log" \
                                   --parallelDataStreams ${PARALLEL_DATA_STREAMS} \
                                   --dataStreamIp="${TARGET_IP}" \
                                   --dataStreamPort="${TARGET_PORT}" 1>/dev/null 2>/dev/null &
    else
        ${PYTHON_CMD} fileMover.py --logfilePath="${LOG_DIR}" \
                                   --bindingIpForSocket="127.0.0.1" \
                                   --logfileName="fileMover.log" \
                                   --parallelDataStreams ${PARALLEL_DATA_STREAMS} \
                                   --dataStreamIp="${TARGET_IP}" \
                                   --dataStreamPort="${TARGET_PORT}" \
                                   --verbose 1>/dev/null 2>/dev/null &
    fi
    local PID=${!}
    echo "starting 'fileMover.py'... started as background job with PID=${PID}"
    PID_FILEMOVER=${PID}

    #to kill process
    #kill -s 24 ${PID}
}


printHelpMessage()
{
    echo ""
}


printOptions()
{
    printf "\
SOURCE_DIR            = %s \n\
TARGET_IP             = %s \n\
TARGET_PORT           = %s \n\
PARALLEL_DATA_STREAMS = %s \n\
PYTHON_CMD            = %s \n\
LOG_DIR               = %s \n\
FILE_MOVER_IP         = %s \n\
FILE_MOVER_PORT       = %s \n\
VERBOSE               = %s \n\
" "${SOURCE_DIR}" \
  "${TARGET_IP}" \
  "${TARGET_PORT}" \
  "${PARALLEL_DATA_STREAMS}" \
  "${PYTHON_CMD}" \
  "${LOG_DIR}" \
  "${FILE_MOVER_IP}" \
  "${FILE_MOVER_PORT}" \
  "${VERBOSE}"

}



argumentParsing()
{
	local ARGUMENT_COUNT=$#


#	if [ $ARGUMENT_COUNT -lt 1 ]
#	then
#	    printHelpMessage
#	    exit 0
#    fi

	while [[ $# > 1 ]]
#	while true
    do
        key="$1"

        case $key in
            -s|--parallelDataStreams)
            PARALLEL_DATA_STREAMS="$2"
            shift
            ;;
            -d|--sourceDirectory)
            SOURCE_DIR="$2"
            shift
            ;;
            -l|--logDir)
            LOG_DIR="$2"
            shift
            ;;
            -fmi|--fileMoverIp)
            FILE_MOVER_IP="$2"
            shift
            ;;
            -fmp|--fileMoverPort)
            FILE_MOVER_PORT="$2"
            shift
            ;;
            -v|--verbose)
            VERBOSE=1
            shift
            ;;
            *)
                    # unknown option
            ;;
        esac
        shift
    done

    return 0
}



checkDependencies()
{
    local FAILED_DEPENDENCIES=0
    #watchdog
    echo "import watchdog" | python 1> /dev/null 2> /dev/null
    local DEPENDENCY_WATCHDOG=${?}
    if [ "${DEPENDENCY_WATCHDOG}" -ne "0" ]
    then
        echo "Missing python library: watchdog"
        FAILED_DEPENDENCIES=1
    fi

    #zeromq
    echo "import zmq" | python 1> /dev/null 2> /dev/null
    local DEPENDENCY_WATCHDOG=${?}
    if [ "${DEPENDENCY_WATCHDOG}" -ne "0" ]
    then
        echo "Missing python library: zmq"
        FAILED_DEPENDENCIES=1
    fi

    #exit on error
    if [ ${FAILED_DEPENDENCIES} == 1 ]
    then
        echo "aborting. missing libraries detected."
        exit 1
    fi
}



argumentParsing $@

checkDependencies

printOptions

startWatchFolder

startFileMover


echo
echo "You can now take data in '${SOURCE_DIR}'"
echo

#get process group of all sub-processed started during the script
PROCESS_GROUP_ID=$(ps x -o  "%p %r %c" | grep ${PID_FILEMOVER} | awk '{print $2}' | head -n1)
echo "to stop execute: kill -- -${PROCESS_GROUP_ID}"
echo
