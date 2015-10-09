import os
import sys
import datetime

BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname (  os.path.realpath ( __file__ ) ) ) )
SRC_PATH  = BASE_PATH + os.sep + "src"

sys.path.append ( SRC_PATH )

import shared.helperScript as helperScript

#LOCAL_IP    = "0.0.0.0"
LOCAL_IP    = "127.0.0.1"
#LOCAL_IP    = "131.169.66.47"

BASE_PATH = "/space/projects/live-viewer"
#BASE_PATH   = "/home/p11user/live-viewer"

now         = datetime.datetime.now()
nowFormated = now.strftime("%Y-%m-%d")

class defaultConfig():

    # where incoming data will be stored to"
    targetDir             = BASE_PATH + "/data/zmq_target"

    # local ip to connect dataStream to
#    dataStreamIp          = LOCAL_IP
#    dataStreamIp          = "131.169.55.170"      # lsdma-lab04.desy.de
#    dataStreamIp          = "131.169.185.121"     # zitpcx19282.desy.de
    dataStreamIp          = "127.0.0.1"
    # tcp port of data pipe"
    dataStreamPort        = "6070"
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

