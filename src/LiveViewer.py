# -*- coding: utf-8 -*-
import os
import sys
import time
#from dectris import albula
from PyQt4 import QtCore
from PyQt4.QtCore import SIGNAL, QThread, QMutex

API_PATH = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) + os.sep + "APIs"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH

from dataTransferAPI import dataTransfer


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

    zmqQuery      = None
#    zmqSignalIp      = "haspp11eval01.desy.de"
    zmqSignalIp   = "zitpcx19282.desy.de"
    zmqDataPort   = "50022"


    def __init__(self, path=None, filetype=None, interval=None, parent=None):
        QThread.__init__(self, parent)
        if path is not None:
            self.path = path
        if filetype is not None:
            self.filetype = filetype
        if interval is not None:
            self.interval = interval

        self.zmqQuery = dataTransfer( self.zmqSignalIp, self.zmqDataPort )
        self.zmqQuery.start("queryMetadata")

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
                receivedFile = self.zmqQuery.get()

                if receivedFile =="None":
                    self.mutex.unlock()
                    continue

                # display image
#                try:
#                    self.subframe.loadFile(receivedFile)
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


if __name__ == '__main__':

    lv = LiveView()

    lv.start()

    time.sleep(100)
    lv.stop()
