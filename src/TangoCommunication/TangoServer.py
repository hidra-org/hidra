#!/usr/bin/env python
#
import threading
import os
import sys
import socket
import subprocess
import logging
from multiprocessing import Queue

try:
    BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
except:
    BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"
CONFIG_PATH = BASE_PATH + os.sep + "conf"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH
del CONFIG_PATH

import helpers
from logutils.queue import QueueHandler



PORT = 51000

BASEDIR = "/root/zeromq-data-transfer"
#BASEDIR = "/space/projects/zeromq-data-transfer"

CONFIGPATH = "/root/zeromq-data-transfer/conf"
#CONFIGPATH = "/space/projects/zeromq-data-transfer/conf"

LOGPATH = "/root/zeromq-data-transfer/logs"
#LOGPATH = "/space/projects/zeromq-data-transfer/logs"

#
# assume that the server listening to 7651 serves p09
#
port2BL = {
    "51000": "p00",
    "51001": "p01",
    "51002": "p02",
    "51003": "p03",
    "51004": "p04",
    "51005": "p05",
    "51006": "p06",
    "51007": "p07",
    "51008": "p08",
    "51009": "p09",
    "51010": "p10",
    "51011": "p11"
    }


class ZmqDT():
    '''
    this class holds getter/setter for all parameters
    and function members that control the operation.
    '''
    def __init__ (self, beamline, log):

        # Beamline is read-only, determined by portNo
        self.beamline = beamline
        self.procname = "zeromq-data-transfer_" + self.beamline

        # Set log handler
        self.log   = log

        # TangoDevices to talk with
        self.detectorDevice = None
        self.filewriterDevice = None

        # TODO replace TangoDevices with the following
        # IP of the EIGER Detector
        # self.eigerIp = None

        # Number of events stored to look for doubles
        self.historySize = None

        # Target to move the files into
        # e.g. /beamline/p11/current/raw
        self.localTarget = None

        # Flag describing if the data should be stored in localTarget
        self.storeData = None

        # Flag describing if the files should be removed from the source
        self.removeData = None

        # List of hosts allowed to connect to the data distribution
        self.whitelist = None


    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger(self.procname)
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def execMsg (self, msg):
        '''
        set filedir /gpfs/current/raw
          returns DONE
        get filedir
          returns /gpfs/current/raw
        do reset
          return DONE
        '''
        tokens = msg.split(' ', 2)

        if len(tokens) == 0:
            return "ERROR"

        if tokens[0].lower() == 'set':
            if len( tokens) < 3:
                return "ERROR"

            return self.set(tokens[1], tokens[2])

        elif tokens[0].lower() == 'get':
            if len( tokens) != 2:
                return "ERROR"

            return self.get(tokens[1])

        elif tokens[0].lower() == 'do':
            if len( tokens) != 2:
                return "ERROR"

            return self.do(tokens[1])

        else:
            return "ERROR"


    def set (self, param, value):
        '''
        set a parameter, e.g.: set filedir /gpfs/current/raw/
        '''

        key = param.lower()

        if key == "detectordevice":
            self.detectorDevice = value
            return "DONE"

        elif key == "filewriterdevice":
            self.filewriterDevice = value
            return "DONE"

#        TODO replace detectordevice and filewriterDevice with eigerIP
#        elif key == "eigerIp":
#            self.eigerIP = value
#            return "DONE"

        elif key == "historysize":
            self.historySize = value
            return "DONE"

        elif key == "localtarget":
            self.localTarget = value
            return "DONE"

        elif key == "storedata":
            self.storeData = value
            return "DONE"

        elif key == "removedata":
            self.removeData = value
            return "DONE"

        elif key == "whitelist":
            self.whitelist = value
            return "DONE"

        else:
            return "ERROR"


    def get (self, param):
        '''
        return the value of a parameter, e.g.: get localtarget
        '''
        key = param.lower()

        if key == "detectordevice":
            self.detectorDevice   = value
            return "DONE"

        elif key == "filewriterdevice":
            self.filewriterDevice = value
            return "DONE"

#        TODO replace detectordevice and filewriterDevice with eigerIP
#        elif key == "eigerIP":
#            return self.eigerIp

        elif key == "historysize":
            return self.historySize

        elif key == "localtarget":
            return self.localTarget

        elif key == "storedata":
            return self.storeData

        elif key == "removedata":
            return self.removeData

        elif key == "whitelist":
            return self.whitelist

        else:
            return "ERROR"


    def do (self, cmd):
        '''
        executes commands
        '''
        key = cmd.lower()

        if key == "start":
            return self.start()

        elif key == "stop":
            return self.stop()

        elif key == "restart":
            return self.restart()

        elif key == "status":
            return self.status()

        else:
            return "ERROR"


    def start (self):
        '''
        start ...
        '''
        #
        # see, if all required params are there.
        #

        if (self.detectorDevice
            and self.filewriterDevice
            # TODO replace TangoDevices with the following
            #and self.eigerIp
            and self.historySize
            and self.localTarget
            and self.storeData
            and self.removeData
            and self.whitelist ):

            #
            # execute the start action ...
            #

            # write configfile
            # /etc/zeromq-data-transfer/P01.conf
            configFile = CONFIGPATH + os.sep + self.beamline + ".conf"
            self.log.info("Writing config file: {f}".format(f=configFile))
            with open(configFile, 'w') as f:
                f.write("logfilePath        = " + LOGPATH                                       + "\n")
                f.write("logfileName        = dataManager.log"                                  + "\n")
                f.write("logfileSize        = 10485760"                                         + "\n")
                f.write("procname           = " + self.procname                                 + "\n")
                f.write("comPort            = 50000"                                            + "\n")
                f.write("requestPort        = 50001"                                            + "\n")

    #            f.write("eventDetectorType  = HttpDetector"                                     + "\n")
                f.write("eventDetectorType  = InotifyxDetector"                                 + "\n")
                f.write('fixSubdirs         = ["commissioning", "current", "local"]'            + "\n")
                f.write("monitoredDir       = " + BASEDIR + "/data/source"                      + "\n")
                f.write('monitoredEvents    = {"IN_CLOSE_WRITE" : [".tif", ".cbf", ".nxs"]}'    + "\n")
                f.write("useCleanUp         = False"                                            + "\n")
                f.write("actionTime         = 150"                                              + "\n")
                f.write("timeTillClosed     = 2"                                                + "\n")

    #            f.write("dataFetcherType    = getFromHttp"                                      + "\n")
                f.write("dataFetcherType    = getFromFile"                                      + "\n")

                f.write("numberOfStreams    = 1"                                                + "\n")
                f.write("useDataStream      = False"                                            + "\n")
                f.write("chunkSize          = 10485760"                                         + "\n")


                f.write("detectorDevice     = " +  str(self.detectorDevice)                     + "\n")
                f.write("filewriterDevice   = " +  str(self.filewriterDevice)                   + "\n")
                # TODO replace TangoDevices with the following
                #f.write("eigerIp            = " +  str(self.eigerIp)                            + "\n")
                f.write("historySize        = " +  str(self.historySize)                        + "\n")
                f.write("localTarget        = " +  str(self.localTarget)                        + "\n")
                f.write("storeData          = " +  str(self.storeData)                          + "\n")
                f.write("removeData         = " +  str(self.removeData)                         + "\n")
                f.write("whitelist          = " +  str(self.whitelist)                          + "\n")

            # check if service is running
            if self.status() == "RUNNING":
                return "ERROR"

            # start service
            p = subprocess.call(["systemctl", "start", "zeromq-data-transfer@" + self.beamline + ".service"])

            if p == 0:
                return "DONE"
            else:
                return "ERROR"

        else:
            self.log.debug("Config file not written")
            self.log.debug("detectorDevice: {d}".format(self.detectorDevice))
            self.log.debug("filewriterDevice: {d}".format(self.filewriterDevice))
            # TODO replace TangoDevices with the following
            #self.log.debug("eigerIp: i{d}".format(self.eigerIp))
            self.log.debug("historySize: {d}".format(self.historySize))
            self.log.debug("localTarge: {d}".format(self.localTarget))
            self.log.debug("storeData: {d}".format(self.storeData))
            self.log.debug("removeData: {d}".format(self.removeData))
            self.log.debug("whitelist: {d}".format(self.whitelist))
            return "ERROR"


    def stop (self):
        # stop service
        p = subprocess.call(["systemctl", "stop", "zeromq-data-transfer@" + self.beamline + ".service"])
        return "DONE"

        if p == 0:
            return "DONE"
        else:
            return "ERROR"


    def restart (self):
        # stop service
        self.stop()

        # start service
        self.start()


    def status (self):
        p = subprocess.call(["systemctl", "is-active", "zeromq-data-transfer@" + self.beamline + ".service"])

        if p == 0:
            return "RUNNING"
        else:
            return "NOT RUNNING"


class socketServer (object):
    '''
    one socket for the port, accept() generates new sockets
    '''

    def __init__ (self, logQueue):
        self.logQueue = logQueue

        self.log      = self.getLogger(logQueue)

        self.host     = socket.gethostname()
        self.port     = PORT # TODO init variable
        self.conns    = []
        self.bl       = port2BL[str(PORT)]
        self.socket   = None

        if not str(PORT) in port2BL.keys():
            raise Exception("Port {p} not identified".format(p=PORT))

        self.createSocket()

    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("socketServer")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def createSocket (self):

        try:
            self.sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.log.debug("Socket created.")
        except Exception:
            self.log.error("Creation of socket failed", exc_info=True)
            sys.exit()

        try:
            self.sckt.bind( (self.host, self.port))
            self.log.info("Start socket (bind):  {h}, {p}".format(h=self.host, p=self.port))
        except Exception:
            self.log.error("Failed to start socket (bind).", exc_info=True)
            raise

        self.sckt.listen(5)


    def run (self):
        while True:
            try:
                conn, addr = self.sckt.accept()

                threading.Thread(target=socketCom, args=(self.logQueue, self.bl, conn, addr)).start()
            except KeyboardInterrupt:
                break
            except Exception, e:
                self.log.error("Stopped due to unknown error", exc_info=True)
                break


    def finish (self):
        if self.sckt:
            self.log.info("Closing Socket")
            self.sckt.close()
            self.sckt = None


    def __exit__ (self):
        self.finish()


    def __del__ (self):
        self.finish()


class socketCom ():
    def __init__ (self, logQueue, bl, conn, addr):
        self.id    = threading.current_thread()

        self.log   = self.getLogger(logQueue)

        self.zmqDT = ZmqDT(bl, self.log)
        self.conn  = conn
        self.addr  = addr

        self.run()


    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("socketCom_" + str(self.id))
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def run (self):

        while True:

            msg = self.recv()

            if len(msg) == 0:
                self.log.debug("Received empty msg")
                continue

            elif msg.lower().find('bye') == 0:
                self.log.debug("Received 'bye'")
                self.close()
                break

            elif msg.find('exit') >= 0:
                self.log.debug("Received 'exit'")
                self.close()
                sys.exit(1)

            reply = self.zmqDT.execMsg (msg)

            if self.send (reply) == 0:
                self.close()
                break


    def recv (self):
        argout = None
        try:
            argout = self.conn.recv(1024)
        except Exception, e:
            print e
            argout = None

        self.log.debug("Recv (len {l: <2}): {m}".format(l=len(argout.strip()), m=argout.strip()))

        return argout.strip()


    def send (self, msg):
        try:
            argout = self.conn.send(msg)
        except:
            argout = ""

        self.log.debug("Send (len {l: <2}): {m}".format(l=argout, m=msg))

        return argout


    def close (self):
        #
        # close the 'accepted' socket only, not the main socket
        # because it may still be in use by another client
        #
        if self.conn:
            self.log.info("Closing connection")
            self.conn.close()
            self.conn = None


    def __exit__ (self):
        self.close()


    def __del__ (self):
        self.close()


class TangoServer():
    def __init__(self):
        onScreen = "debug"
        verbose  = True
        logfile  = BASE_PATH + os.sep + "logs" + os.sep + "tangoServer.log"
        logsize  = 10485760

        # Get queue
        self.logQueue    = Queue(-1)

        # Get the log Configuration for the lisener
        if onScreen:
            h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose, onScreen)

            # Start queue listener using the stream handler above.
            self.logQueueListener = helpers.CustomQueueListener(self.logQueue, h1, h2)
        else:
            h1 = helpers.getLogHandlers(logfile, logsize, verbose, onScreen)

            # Start queue listener using the stream handler above
            self.logQueueListener = helpers.CustomQueueListener(self.logQueue, h1)

        self.logQueueListener.start()

        # Create log and set handler to queue handle
        self.log = self.getLogger(self.logQueue)

        self.log.info("Init")

        # waits for new accepts on the original socket,
        # receives the newly created socket and
        # creates threads to handle each client separatly
        s = socketServer(self.logQueue)

        s.run()


    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("TangoServer")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


if __name__ == '__main__':
    t = TangoServer()

