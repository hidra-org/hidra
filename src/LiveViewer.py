# -*- coding: utf-8 -*-
import os
import time
#from dectris import albula
from PyQt4 import QtCore
from PyQt4.QtCore import SIGNAL, QThread, QMutex
import zmq
import socket       # needed to get hostname

class LiveView(QThread):
    FILETYPE_CBF = 0
    FILETYPE_TIF = 1
    FILETYPE_HDF5 = 2

    alive = False
    path = ""
    filetype = 0
    interval = 0.5 #s
    stoptimer = -1.0

    viewer = None
    subframe = None
    mutex = None

    zmqContext = None

    hostname = socket.gethostname()
#    zmqComIp      = "haspp11eval01.desy.de"
    zmqComIp      = "zitpcx19282.desy.de"
    zmqComPort    = "50021"
#    zmqDataIp     = "haspp11eval01.desy.de"
    zmqDataIp     = "0.0.0.0"
    zmqDataPort   = "50022"

    zmqComSocket  = None
    zmqDataSocket = None

    def __init__(self, path=None, filetype=None, interval=None, parent=None):
        QThread.__init__(self, parent)
        if path is not None:
            self.path = path
        if filetype is not None:
            self.filetype = filetype
        if interval is not None:
            self.interval = interval

        self.zmqContext = createZmqContext()
        self.zmqComSocket  = createZmqSocket(self.zmqContext, self.zmqComIp, self.zmqComPort, "connect")
        self.zmqDataSocket = createZmqSocket(self.zmqContext, self.zmqDataIp, self.zmqDataPort, "bind")
        establishDataExchange(self.zmqComSocket, self.hostname, self.zmqDataPort)

        self.mutex = QMutex()


    def start(self, path=None, filetype=None, interval=None):
        if path is not None:
            self.path = path
        if filetype is not None:
            self.filetype = filetype
        if interval is not None:
            self.interval = interval
        QThread.start(self)


    def stop(self, interval=0.0):
        if self.stoptimer < 0.0 and interval > 0.0:
            print "Live view thread: Stopping in %d seconds"%interval
            self.stoptimer = interval
            return
        print "Live view thread: Stopping thread"
        self.alive = False

        # close ZeroMQ socket and destroy ZeroMQ context
        stopZmq(self.zmqComSocket, self.zmqDataSocket, self.hostname, self.zmqDataPort, self.zmqContext)

        self.wait() # waits until run stops on his own



    def run(self):
        self.alive = True
        print "Live view thread: started"
        suffix = [".cbf", ".tif", ".hdf5"]

        if self.filetype in [LiveView.FILETYPE_CBF, LiveView.FILETYPE_TIF]:
            # open viewer
            while self.alive:
                # find latest image
                self.mutex.lock()

                # get latest file from reveiver
                try:
                    received_file = communicateWithReceiver(self.zmqDataSocket)
                    print "===received_file", received_file
                except zmq.error.ZMQError:
                    received_file = None
                    print "ZMQError"
                    break

                # display image
#                try:
#                    self.subframe.loadFile(receiived_file)
                # viewer or subframe has been closed by the user
#                except:
#                    self.mutex.unlock()
#                    time.sleep(0.1)
#                    try:
#                        self.subframe = self.viewer.openSubFrame()
#                    except:
#                        self.viewer = albula.openMainFrame()
#                        self.subframe = self.viewer.openSubFrame()
#                    continue
                self.mutex.unlock()
                # wait interval
                interval = 0.0
                while interval < self.interval and self.alive:
                    if self.stoptimer > 0.0:
                        self.stoptimer -= 0.05
                        if self.stoptimer < 0.0:
                            self.stoptimer = -1.0
                            self.alive = False
                    time.sleep(0.05)
                    interval += 0.05
        elif self.filetype == LiveView.FILETYPE_HDF5:
            print "Live view thread: HDF5 not supported yet"

        print "Live view thread: Thread for Live view died"
        self.alive = False

    def setPath(self, path=None):
        self.mutex.lock()
        if path is not None:
            self.path = path
        self.mutex.unlock()

    def setFiletype(self, filetype=None):
        restart = False
        if self.alive:
            restart = True
            self.stop()
        if filetype is not None:
            self.filetype = filetype
        if restart:
            self.start()

    def setInterval(self, interval=None):
        if interval is not None:
            self.interval = interval


def createZmqContext():
    return zmq.Context()


def createZmqSocket(context, zmqIp, zmqPort, connectType):
    if connectType not in ["connect", "bind"]:
        print "Sockets can only be bound or connected to."
        return None

    socket = context.socket(zmq.REQ)
    connectionStr = "tcp://{ip}:{port}".format(ip=zmqIp, port=zmqPort)
    print connectionStr

    if connectType == "connect":
        socket.connect(connectionStr)
    elif connectType == "bind":
        socket.bind(connectionStr)

    return socket


def establishDataExchange(zmqSocket, hostname, dataPort):
    print "Sending Start Signal to receiver"
    sendMessage = "START_DISPLAYER," + str(hostname) + "," + str(dataPort)
    try:
        zmqSocket.send (sendMessage)
        #  Get the reply.
        message = zmqSocket.recv()
        print "Recieved signal: ", message
    except Exception as e:
        print "Could not communicate with receiver"
        print "Error was: ", e


def communicateWithReceiver(zmqSocket):
    print "Asking for next file"

    sendMessage = "NEXT_FILE"
    print "sendMessage", sendMessage
    try:
        print "Sending"
        zmqSocket.send (sendMessage)
    except Exception as e:
        print "Could not communicate with receiver"
        print "Error was: ", e
        return ""

    try:
        #  Get the reply.
        print "Receiving"
        message = zmqSocket.recv()
        print "Next file: ", message
    except Exception as e:
        message = ""
        print "Could not communicate with receiver"
        print "Error was: ", e

    return message


def stopZmq(zmqComSocket, zmqDataSocket, hostname, dataPort, zmqContext):

    print "Sending Start Signal to receiver"
    sendMessage = "STOP_DISPLAYER," + str(hostname) + "," + str(dataPort)
    try:
        zmqComSocket.send (sendMessage)
        #  Get the reply.
        message = zmqComSocket.recv()
        print "Recieved signal: ", message
    except Exception as e:
        print "Could not communicate with receiver"
        print "Error was: ", e

    try:
        print "closing ZMQ sockets..."
        zmqComSocket.close(linger=0)
        zmqDataSocket.close(linger=0)
        print "closing ZMQ Sockets...done."
    except Exception as e:
        print "closing ZMQ Sockets...failed."
        print e

    try:
        print"closing zmqContext..."
        zmqContext.destroy()
        "closing zmqContext...done."
    except Exception as e:
        print "closing zmqContext...failed."
        print e



if __name__ == '__main__':

    lv = LiveView()

    lv.start()

    time.sleep(100)
    lv.stop()
