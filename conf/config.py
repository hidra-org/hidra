import os
import sys
import datetime

BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname (  os.path.realpath ( __file__ ) ) ) )
ZMQ_PATH  = BASE_PATH + os.sep + "src" + os.sep + "ZeroMQTunnel"

sys.path.append ( ZMQ_PATH )

import helperScript

#LOCAL_IP    = "0.0.0.0"
#LOCAL_IP    = "127.0.0.1"
LOCAL_IP    = "131.169.66.47"

#BASE_PATH = "/space/projects/live-viewer"
BASE_PATH   = "/home/p11user/live-viewer"

now         = datetime.datetime.now()
nowFormated = now.strftime("%Y-%m-%d")

class defaultConfigSender():

    # folder you want to monitor for changes
    # inside this folder only the subdirectories "commissioning", "current" and "local" are monitored
#    watchFolder         = BASE_PATH + "/data/source/"
    watchFolder         = "/rd/"
    watchFolder         = "/rd_temp/"
    # Target to move the files into
#    cleanerTargetPath   = BASE_PATH + "/data/target/"
    cleanerTargetPath   = "/gpfs/"
#    cleanerTargetPath   = "/rd/temp/"

    # subfolders of watchFolders to be monitored
    monitoredSubfolders = ["commissioning", "current", "local"]
    # the formats to be monitored, files in an other format will be be neglected
    monitoredFormats    = (".tif", ".cbf")

    # number of parallel data streams
    parallelDataStreams = "1"

    # list of hosts allowed to connect to the sender
#    receiverWhiteList   = ["lsdma-lab04"]
#    receiverWhiteList   = ["zitpcx19282"]
    receiverWhiteList   = ["zitpcx19282", "zitpcx22614", "lsdma-lab04" , "haspp11eval01" , "it-hpc-cxi04", "it-hpc-cxi03" ]

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
    cleanerIp           = LOCAL_IP
    # zmq-pull-socket port which deletes/moves given files
    cleanerPort         = "6062"
    # port number of dataStream-socket to receive signals from the receiver
    receiverComPort     = "6080"
    # ports and ips to communicate with onda/realtime analysis
    # there needs to be one entry for each workerProcess (meaning streams)
    ondaIps             = ["0.0.0.0"]
    ondaPorts           = ["6081"]

    # chunk size of file-parts getting send via zmq
    #chunkSize           = 1048576 # = 1024*1024
    chunkSize           = 10485760 # = 1024*1024*10
    #chunkSize           = 1073741824 # = 1024*1024*1024

#    # path where logfile will be created
#    if helperScript.isWindows():
#        logfilePath = "C:\\"
#    elif helperScript.isLinux():
#        logfilePath = BASE_PATH + "/logs"

    # path where logfile will be created
    logfilePath         = "/home/p11user/logs"
#    logfilePath = BASE_PATH + "/logs"

    # filename used for logging
    logfileName         = "zmq_sender.log_" + nowFormated


    def __init__(self):
        # check if folders exists
        helperScript.checkFolderExistance(self.logfilePath)
        helperScript.checkFolderExistance(self.watchFolder)
        helperScript.checkFolderExistance(self.cleanerTargetPath)

        # check if logfile is writable
        helperScript.checkLogFileWritable(self.logfilePath, self.logfileName)


class defaultConfigReceiver():

    # where incoming data will be stored to"
    targetDir             = BASE_PATH + "/data/zmq_target"

    # local ip to connect dataStream to
#    dataStreamIp          = LOCAL_IP
#    dataStreamIp          = "131.169.55.170"      # lsdma-lab04.desy.de
#    dataStreamIp          = "131.169.185.121"     # zitpcx19282.desy.de
    dataStreamIp          = "127.0.0.1"
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
    logfilePath       = BASE_PATH + "/logs"
#    logfilePath         = "/home/p11user/logs"
    # filename used for logging
    logfileName       = "zmq_receiver.log_" + nowFormated
    # size of the ring buffer for the live viewer
    maxRingBufferSize = 10


    def __init__(self):
        # check if folders exists
        helperScript.checkFolderExistance(self.targetDir)

        # check if logfile is writable
        helperScript.checkLogFileWritable(self.logfilePath, self.logfileName)
