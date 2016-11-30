#!/usr/bin/env python
#
import time
import threading
import os
import sys
import socket
import subprocess
import logging
import argparse
import setproctitle
from multiprocessing import Queue
import tempfile

try:
    BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
except:
    BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")
CONFIG_PATH = os.path.join(BASE_PATH, "conf")

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH
del CONFIG_PATH

import helpers
from logutils.queue import QueueHandler


BASEDIR = "/opt/hidra"

CONFIGPATH = "/opt/hidra/conf"

LOGPATH = os.path.join(tempfile.gettempdir(), "hidra", "logs")

#
# assume that the server listening to 51000 serves p00
#
connectionList = {
    "p00": {
        "host" : "asap3-p00",
        "port" : 51000 },
    "p01": {
        "host" : "asap3-bl-prx07",
        "port" : 51001 },
    "p02.1": {
        "host" : "asap3-bl-prx07",
        "port" : 51002 },
    "p02.2": {
        "host" : "asap3-bl-prx07",
        "port" : 51003 },
    "p03": {
        "host" : "asap3-bl-prx07",
        "port" : 51004 },
    "p04": {
        "host" : "asap3-bl-prx07",
        "port" : 51005 },
    "p05": {
        "host" : "asap3-bl-prx07",
        "port" : 51006 },
    "p06": {
        "host" : "asap3-bl-prx07",
        "port" : 51007 },
    "p07": {
        "host" : "asap3-bl-prx07",
        "port" : 51008 },
    "p08": {
        "host" : "asap3-bl-prx07",
        "port" : 51009 },
    "p09": {
        "host" : "asap3-bl-prx07",
        "port" : 51010 },
    "p10": {
        "host" : "asap3-bl-prx07",
        "port" : 51011 },
    "p11": {
        "host" : "asap3-bl-prx07",
        "port" : 51012 },
    }



class ZmqDT():
    '''
    this class holds getter/setter for all parameters
    and function members that control the operation.
    '''
    def __init__ (self, beamline, log):

        # Beamline is read-only, determined by portNo
        self.beamline = beamline
        self.procname = "hidra_" + self.beamline

        # Set log handler
        self.log   = log

        # TODO remove TangoDevices
        # TangoDevices to talk with
        self.detectorDevice = None
        self.filewriterDevice = None

        #TODO after removal of detectorDevice and filewriterDevice: change default to None
        # IP of the EIGER Detector
        self.eigerIp = "None"
        # API version of the EIGER Detector
        self.eigerApiVersion = "None"

        # Number of events stored to look for doubles
        self.historySize = None

        # Target to move the files into
        # e.g. /beamline/p11/current/raw
        self.localTarget = None
        self.supportedLocalTargets = ["current/raw", "current/scratch_bl", "commissioning/raw", "commissioning/scratch_bl", "local"]

        # Flag describing if the data should be stored in localTarget
        self.storeData = None

        # Flag describing if the files should be removed from the source
        self.removeData = None

        # List of hosts allowed to connect to the data distribution
        self.whitelist = None


    def get_logger (self, queue):
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
            if len(tokens) < 3:
                return "ERROR"

            return self.set(tokens[1], tokens[2])

        elif tokens[0].lower() == 'get':
            if len(tokens) != 2:
                return "ERROR"

            return self.get(tokens[1])

        elif tokens[0].lower() == 'do':
            if len(tokens) != 2:
                return "ERROR"

            return self.do(tokens[1])

        else:
            return "ERROR"


    def set (self, param, value):
        '''
        set a parameter, e.g.: set localTarget /beamline/p11/current/raw/
        '''

        key = param.lower()

        if key == "eigerip":
            self.eigerIp = value
            return "DONE"

        elif key == "eigerapiversion":
            self.eigerApiVersion = value
            return "DONE"

        elif key == "historysize":
            self.historySize = value
            return "DONE"

        elif key == "localtarget" and value in self.supportedLocalTargets:
            self.localTarget = os.path.join("/beamline", self.beamline, value)
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
            self.log.debug("key={a}; value={v}".format(a=key, v=value))
            return "ERROR"


    def get (self, param):
        '''
        return the value of a parameter, e.g.: get localtarget
        '''
        key = param.lower()

        if key == "eigerip":
            return self.eigerIp

        elif key == "eigerapiversion":
            return self.eigerApiVersion

        elif key == "historysize":
            return self.historySize

        elif key == "localtarget":
            return os.path.relpath(self.localTarget, os.path.join("/beamline", self.beamline))

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

    def __writeConfig (self):
        global CONFIGPATH

        #
        # see, if all required params are there.
        #
        if (self.eigerIp
            and self.eigerApiVersion
            and self.historySize
            and self.localTarget
            and self.storeData != None
            and self.removeData != None
            and self.whitelist ):

            #TODO correct IP
            if self.beamline == "p00":
                externalIp    = "asap3-p00"
#                externalIp    = "131.169.251.55" # asap3-p00
                eventDetector = "InotifyxDetector"
                dataFetcher   = "getFromFile"
            else:
                externalIp    = "asap3-bl-prx07"
#                externalIp    = "131.169.251.38" # asap3-bl-prx07
                eventDetector = "HttpDetector"
                dataFetcher   = "getFromHttp"

            # write configfile
            # /etc/hidra/P01.conf
            configFile = CONFIGPATH + os.sep + self.beamline + ".conf"
            self.log.info("Writing config file: {0}".format(configFile))

            with open(configFile, 'w') as f:
                f.write("logfilePath        = {0}\n".format(LOGPATH))
                f.write("logfileName        = dataManager_{0}.log\n".format(self.beamline))
                f.write("logfileSize        = 10485760\n")
                f.write("procname           = {0}\n".format(self.procname))
                f.write("extIp              = {0}\n".format(externalIp))
                f.write("comPort            = 50000\n")
                f.write("requestPort        = 50001\n")

                f.write("eventDetectorType  = {0}\n".format(eventDetector))
                f.write('fixSubdirs         = ["commissioning", "current", "local"]\n')
                f.write("monitoredDir       = {0}/data/source\n".format(BASEDIR))
                f.write('monitoredEvents    = {"IN_CLOSE_WRITE" : [".tif", ".cbf", ".nxs"]}\n')
                f.write("useCleanUp         = False\n")
                f.write("actionTime         = 150\n")
                f.write("timeTillClosed     = 2\n")

                f.write("dataFetcherType    = {0}\n".format(dataFetcher))

                f.write("numberOfStreams    = 1\n")
                f.write("useDataStream      = False\n")
                f.write("chunkSize          = 10485760\n")

                f.write("eigerIp            = {0}\n".format(self.eigerIp))
                f.write("eigerApiVersion    = {0}\n".format(self.eigerApiVersion))
                f.write("historySize        = {0}\n".format(self.historySize))
                f.write("localTarget        = {0}\n".format(self.localTarget))
                f.write("storeData          = {0}\n".format(self.storeData))
                f.write("removeData         = {0}\n".format(self.removeData))
                f.write("whitelist          = {0}\n".format(self.whitelist))

                self.log.debug("Started with extIp: {0}".format(externalIp))
                self.log.debug("Started with eventDetector: {0}".format(eventDetector))
                self.log.debug("Started with dataFetcher: {0}".format(dataFetcher))


        else:
            self.log.debug("eigerIp: {0}".format(self.eigerIp))
            self.log.debug("eigerApiVersion: {0}".format(self.eigerApiVersion))
            self.log.debug("historySize: {0}".format(self.historySize))
            self.log.debug("localTarge: {0}".format(self.localTarget))
            self.log.debug("storeData: {0}".format(self.storeData))
            self.log.debug("removeData: {0}".format(self.removeData))
            self.log.debug("whitelist: {0}".format(self.whitelist))
            raise Exception("Not all required parameters are specified")


    def start (self):
        '''
        start ...
        '''

        try:
            self.__writeConfig()
        except:
            self.log.error("ConfigFile not written", exc_info)
            return "ERROR"

        # check if service is running
        if self.status() == "RUNNING":
            return "ERROR"

        # start service
        p = subprocess.call(["systemctl", "start", "hidra@" + self.beamline + ".service"])

        if p != 0:
            return "ERROR"

        # Needed because status always returns "RUNNING" in the first second
        time.sleep(1)

        # check if really running before return
        if self.status() == "RUNNING":
            return "DONE"
        else:
            return "ERROR"


    def stop (self):
        # stop service
        p = subprocess.call(["systemctl", "stop", "hidra@" + self.beamline + ".service"])
        return "DONE"

        if p == 0:
            return "DONE"
        else:
            return "ERROR"


    def restart (self):
        # stop service
        reval = self.stop()

        if reval == "DONE":
            # start service
            return self.start()
        else:
            return "ERROR"



    def status (self):
        try:
            p = subprocess.call(["systemctl", "is-active", "hidra@" + self.beamline + ".service"])
        except:
            return "ERROR"

        if p == 0:
            return "RUNNING"
        else:
            return "NOT RUNNING"


class socketServer (object):
    '''
    one socket for the port, accept() generates new sockets
    '''
    global connectionList

    def __init__ (self, logQueue, beamline):
        global connectionList

        self.logQueue = logQueue

        self.log      = self.get_logger(logQueue)

        self.beamline = beamline
        self.log.debug('socketServer startet for beamline {bl}'.format(bl=self.beamline))

        self.host     = connectionList[self.beamline]["host"]
        self.port     = connectionList[self.beamline]["port"]
        self.conns    = []
        self.socket   = None

        self.create_socket()


    def get_logger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("socketServer")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def create_socket (self):

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

                threading.Thread(target=socketCom, args=(self.logQueue, self.beamline, conn, addr)).start()
            except KeyboardInterrupt:
                break
            except Exception as e:
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
    def __init__ (self, logQueue, beamline, conn, addr):
        self.id    = threading.current_thread().name

        self.log   = self.get_logger(logQueue)

        self.zmqDT = ZmqDT(beamline, self.log)
        self.conn  = conn
        self.addr  = addr

        self.run()


    def get_logger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("socketCom_{0}".format(self.id))
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def run (self):

        while True:

            msg = self.recv()

            # These calls return the number of bytes received, or -1 if an error
            # occurred.The return value will be 0 when the peer has performed an
            # orderly shutdown.
            # see: http://man7.org/linux/man-pages/man2/recv.2.html
            if len(msg) == 0:
                self.log.debug("Received empty msg")
                break

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
        except Exception as e:
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


def argument_parsing():
    parser = argparse.ArgumentParser()

    parser.add_argument("--beamline"          , type    = str,
                                                help    = "Beamline for which the HiDRA Server for the Eiger detector should be started",
                                                default = "p00")
    return parser.parse_args()


class HiDRAControlServer():
    def __init__(self):
        arguments = argument_parsing()

        self.beamline = arguments.beamline

        setproctitle.setproctitle("HiDRAControlServer_{0}".format(self.beamline))

        onScreen = False
#        onScreen = "debug"
        verbose  = True
        logfile  = os.path.join(BASE_PATH, "logs", "HiDRAControlServer_{0}.log".format(self.beamline))
        logsize  = 10485760

        # Get queue
        self.logQueue    = Queue(-1)

        # Get the log Configuration for the lisener
        if onScreen:
            h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose, onScreen)

            # Start queue listener using the stream handler above.
            self.logQueueListener = helpers.CustomQueueListener(self.logQueue, h1, h2)
        else:
            h1 = helpers.get_log_handlers(logfile, logsize, verbose, onScreen)

            # Start queue listener using the stream handler above
            self.logQueueListener = helpers.CustomQueueListener(self.logQueue, h1)

        self.logQueueListener.start()

        # Create log and set handler to queue handle
        self.log = self.get_logger(self.logQueue)

        self.log.info("Init")

        # waits for new accepts on the original socket,
        # receives the newly created socket and
        # creates threads to handle each client separatly
        s = socketServer(self.logQueue, self.beamline)

        s.run()


    def get_logger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("HiDRAControlServer")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


if __name__ == '__main__':
    t = HiDRAControlServer()

