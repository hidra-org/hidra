# -*- coding: utf-8 -*-
import os
import time
#from dectris import albula
from PyQt4 import QtCore
from PyQt4.QtCore import SIGNAL, QThread, QMutex
import zmq

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

    zmqIp = "127.0.0.1"
    zmqPort = "6071"
    zmqContext = None
    zmqSocket = None

    def __init__(self, path=None, filetype=None, interval=None, parent=None):
        QThread.__init__(self, parent)
        if path is not None:
            self.path = path
        if filetype is not None:
            self.filetype = filetype
        if interval is not None:
            self.interval = interval
        self.zmqContext, self.zmqSocket = createZmqSocket(self.zmqIp, self.zmqPort)
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

        self.wait() # waits until run stops on his own

        # close ZeroMQ socket and destroy ZeroMQ context
        stopZmq(self.zmqSocket, self.zmqContext)


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
                    received_file = communicateWithReceiver(self.zmqSocket)
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


def createZmqSocket(zmqIp, zmqPort):
    context = zmq.Context()
    assert isinstance(context, zmq.sugar.context.Context)

    socket = context.socket(zmq.REQ)
    connectionStrSocket = "tcp://{ip}:{port}".format(ip=zmqIp, port=zmqPort)
    socket.connect(connectionStrSocket)

    return context, socket

def communicateWithReceiver(socket):
    print "Asking for next file"
    socket.send ("NextFile")
    #  Get the reply.
    message = socket.recv()
    print "Next file: ", message
    return message

def stopZmq(zmqSocket, zmqContext):
    try:
        print "closing zmqSocket..."
        zmqSocket.close(linger=0)
        print "closing zmqSocket...done."
    except Exception as e:
        print "closing zmqSocket...failed."
        print e

    try:
        print"closing zmqContext..."
        zmqContext.destroy()
        "closing zmqContext...done."
    except Exception as e:
        print "closing zmqContext...failed."
        print e

