#!/usr/bin/env python
#
import thread
import os
import socket
import subprocess
import psutil

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
    def __init__ (self, beamline):

        # Beamline is read-only, determined by portNo
        self.beamline = beamline
        self.procname = "zeromq-data-transfer_" + self.beamline

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


    def execMsg (self, msg):
        '''
        set filedir /gpfs/current/raw
          returns DONE
        get filedir
          returns /gpfs/current/raw
        do reset
          return DONE
        '''
        tokens = msg.split(' ')

        if len( tokens) == 0:
            return "ERROR"

        if tokens[0].lower() == 'set':
            if len( tokens) != 3:
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
            return self.stop()

        elif key == "status":
            return self.stop()

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
                f.write("monitoredEventType = IN_CLOSE_WRITE"                                   + "\n")
                f.write('monitoredFormats   = [".tif", ".cbf"]'                                 + "\n")
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
            # psutil version 4
            if self.procname in [psutil.Process(i).name() for i in psutil.pids()]:
            # psutil version 1
#            if self.procname in [psutil.Process(i).name for i in psutil.get_pid_list()]:
                print self.procname + " is already running"
                return "ERROR"

            # start service
            p = subprocess.call(["systemctl", "start", "zeromq-data-transfer@" + self.beamline + ".service"])
            print "returncode=", p

            if p == 0:
                return "DONE"
            else:
                return "ERROR"

        else:
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
        p = subprocess.call(["systemctl", "stop", "zeromq-data-transfer@" + self.beamline + ".service"])

        # start service
        p = subprocess.call(["systemctl", "start", "zeromq-data-transfer@" + self.beamline + ".service"])
        print "returncode=", p

        if p == 0:
            return "DONE"
        else:
            return "ERROR"


    def status (self):
        if self.procname in [psutil.Process(i).name() for i in psutil.pids()]:
        # psutil version 1
#            if self.procname in [psutil.Process(i).name for i in psutil.get_pid_list()]:
            print self.procname + " is already running"
            return "RUNNING"
        else:
            return "NOT RUNNING"


class sockel (object):
    '''
    one socket for the port, accept() generates new sockets
    '''
    sckt = None

    def __init__ (self):
        self.conn   = None
        self.addr   = None
        self.host   = socket.gethostname()
        self.port   = PORT # TODO init variable

        if sockel.sckt is None:
            self.createSocket()

        self.conn, self.addr = sockel.sckt.accept()

        if not str(PORT) in port2BL.keys():
            raise Exception( "sockel.__init__: port %d not identified" % str(PORT))

        self.zmqDT = ZmqDT( port2BL[ str(PORT)])


    def createSocket (self):
        print "create the main socket"
        try:
            sockel.sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except Exception, e:
            print "socket() failed", e
            sys.exit()
        try:
            sockel.sckt.bind( (self.host, self.port))
        except Exception, e:
            raise Exception( "sockel.__init__: bind() failed %s" % str(e))

        print "bind( %s, %d) ok" % (self.host, self.port)
        sockel.sckt.listen(5)


    def close (self):
        #
        # close the 'accepted' socket only, not the main socket
        # because it may still be in use by another client
        #
        if not self.conn is None:
            self.conn.close()


    def finish (self):
        sockel.sckt.close()


    def recv (self):
        argout = None
        try:
            argout = self.conn.recv(1024)
        except Exception, e:
            print e
            argout = None

        print "recv (len %2d): %s" % (len( argout.strip()), argout.strip())

        return argout.strip()


    def send (self, msg):
        try:
            argout = self.conn.send( msg)
        except:
            argout = ""

        print "sent (len %2d): %s" % (argout, msg)

        return argout


def socketAcceptor ():
    # waits for new accepts on the original socket,
    # receives the newly created socket and
    # creates threads to handle each client separatly
    while True:
        s = sockel()
        print "socketAcceptor: new connect"
        thread.start_new_thread( socketServer, (s, ))


def socketServer (s):

    while True:

        msg = s.recv()

        if len(msg) == 0:
            print "received empty msg"
            continue

        elif msg.lower().find('bye') == 0:
            print "received 'bye', closing socket"
            s.close()
            break

        elif msg.find('exit') >= 0:
            s.close()
            s.finish()
            os._exit(1)

        reply = s.zmqDT.execMsg (msg)

        if s.send (reply) == 0:
            s.close()
            break


if __name__ == '__main__':
    socketAcceptor()

