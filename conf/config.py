import os
import sys

BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname (  os.path.realpath ( __file__ ) ) ) )
ZMQ_PATH  = BASE_PATH + os.sep + "src" + os.sep + "ZeroMQTunnel"

sys.path.append ( ZMQ_PATH )

import helperScript

#LOCAL_IP= "0.0.0.0"
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
#    dataStreamIp        = LOCAL_IP
    dataStreamIp        = "0.0.0.0"
    # port number of dataStream-socket to push new files to
    dataStreamPort      = "6061"
    # zmq-pull-socket ip which deletes/moves given files
    zmqCleanerIp        = LOCAL_IP
    # zmq-pull-socket port which deletes/moves given files
    zmqCleanerPort      = "6062"
    # port number of dataStream-socket to receive signals from the receiver
    receiverComPort     = "6080"
    # chunk size of file-parts getting send via zmq
    chunkSize           = 1048576 # = 1024*1024
    #chunkSize           = 1073741824 # = 1024*1024*1024

#    # path where logfile will be created
#    if helperScript.isWindows():
#        logfilePath = "C:\\"
#    elif helperScript.isLinux():
#        logfilePath = "/space/projects/live-viewer/logs"

    # path where logfile will be created
    logfilePath         = "/space/projects/live-viewer/logs"
    # filename used for logging
    logfileName         = "zmq_sender.log"


    def __init__(self):
        # check if folders exists
        helperScript.checkFolderExistance(self.logfilePath)
        helperScript.checkFolderExistance(self.watchFolder)
        helperScript.checkFolderExistance(self.cleanerTargetPath)

        # check if logfile is writable
        helperScript.checkLogFileWritable(self.logfilePath, self.logfileName)


class defaultConfigReceiver():

    # where incoming data will be stored to"
    targetDir             = "/space/projects/live-viewer/data/zmq_target"

    # local ip to connect dataStream to
#    dataStreamIp          = LOCAL_IP
#    dataStreamIp          = "131.169.55.170"      # lsdma-lab04.desy.de
    dataStreamIp          = "131.169.185.121"     # zitpcx19282.desy.de
    # tcp port of data pipe"
    dataStreamPort        = "6061"
    # local ip to bind LiveViewer to
    liveViewerIp          = LOCAL_IP
    # tcp port of live viewer"
    liveViewerPort        = "6071"

    # port number of dataStream-socket to send signals back to the sender
    senderComPort         = "6080"
    # time to wait for the sender to give a confirmation of the signal
    senderResponseTimeout = 1000

    # path where logfile will be created
    logfilePath       = "/space/projects/live-viewer/logs"
    # filename used for logging
    logfileName       = "zmq_receiver.log"
    # size of the ring buffer for the live viewer
    maxRingBufferSize = 100


    def __init__(self):
        # check if folders exists
        helperScript.checkFolderExistance(self.targetDir)

        # check if logfile is writable
        helperScript.checkLogFileWritable(self.logfilePath, self.logfileName)

