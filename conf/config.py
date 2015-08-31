import os
import sys

BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname (  os.path.realpath ( __file__ ) ) ) )
ZMQ_PATH  = BASE_PATH + os.sep + "src" + os.sep + "ZeroMQTunnel"

sys.path.append ( ZMQ_PATH )

import helperScript

LOCAL_IP= "127.0.0.1"

class defaultConfigSender():

    # folder you want to monitor for changes
    watchFolder         = "/space/projects/live-viewer/data/source/"
    # Target to move the files into
    cleanerTargetPath   = "/space/projects/live-viewer/data/target/"
    # number of parallel data streams
    parallelDataStreams = "1"

    # zmq endpoint (IP-address) to send file events to
    fileEventIp         = LOCAL_IP
    # zmq endpoint (port) to send file events to
    fileEventPort       = "6060"
    # ip of dataStream-socket to push new files to
    dataStreamIp        = LOCAL_IP
    # port number of dataStream-socket to push new files to
    dataStreamPort      = "6061"
    # zmq-pull-socket ip which deletes/moves given files
    zmqCleanerIp        = LOCAL_IP
    # zmq-pull-socket port which deletes/moves given files
    zmqCleanerPort      = "6063"
    #chunk size of file-parts getting send via zmq
    chunkSize           = 1048576 # = 1024*1024
    #chunkSize           = 1073741824 # = 1024*1024*1024

    fileWaitTimeInMs    = 2000
    fileMaxWaitTimeInMs = 10000

    #filename used for logging
    logfileName         = "watchFolder.log"

    # path where logfile will be created
    if helperScript.isWindows():
        logfilePath = "C:\\"
    elif helperScript.isLinux():
        logfilePath = "/space/projects/live-viewer/logs"


    def __init__(self):
        # check if folders exists
        helperScript.checkFolderExistance(self.logfilePath)
        helperScript.checkFolderExistance(self.watchFolder)
        helperScript.checkFolderExistance(self.cleanerTargetPath)

        # check if logfile is writable
        helperScript.checkLogFileWritable(self.logfilePath, self.logfileName)

