# -*- coding: utf-8 -*-
from __future__ import print_function
#from __future__ import unicode_literals

import os
import time
import socket
#from PyQt4 import QtCore
from PyQt4.QtCore import QThread, QMutex

from __init__ import BASE_PATH

from hidra import Transfer

#from dectris import albula


class LiveView(QThread):
    FILETYPE_CBF = 0
    FILETYPE_TIF = 1
    FILETYPE_HDF5 = 2

    alive = False
    path = ""
    filetype = 0
    interval = 0.5  # s
    stoptimer = -1.0

    viewer = None
    subframe = None
    mutex = None

    query = None
#    transfer_type = "haspp11eval01.desy.de"
    transfer_type = "zitpcx19282.desy.de"
#    transfer_type = "zitpcx22614w.desy.de"
    data_port = "50022"
    basepath = os.path.join(BASE_PATH, "data", "target")
#    basepath = "/gpfs"

    def __init__(self, path=None, filetype=None, interval=None, parent=None):
        QThread.__init__(self, parent)
        if path is not None:
            self.path = path
        if filetype is not None:
            self.filetype = filetype
        if interval is not None:
            self.interval = interval

#        suffix = [".cbf", ".tif", ".hdf5"]

        self.query = Transfer("queryMetadata", self.transfer_type)
        self.query.initiate([socket.gethostname(), self.data_port, "1"])
        self.query.start(self.data_port)
#        self.query.initiate(["zitpcx22614w", self.data_port, "1"])
#        self.query.start(["zitpcx22614w", self.data_port])

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
            print ("Live view thread: Stopping in %d seconds" % interval)
            self.stoptimer = interval
            return
        print ("Live view thread: Stopping thread")
        self.alive = False

        self.wait()  # waits until run stops on his own

    def run(self):
        self.alive = True
        print ("Live view thread: started")

        if self.filetype in [LiveView.FILETYPE_CBF, LiveView.FILETYPE_TIF]:
            # open viewer
            while self.alive:
                # find latest image
                self.mutex.lock()

                # get latest file from reveiver
                [metadata, data] = self.query.get(2000)

                receivedFile = (
                    self.query.generate_target_filepath(self.basepath,
                                                        metadata))
                print ("Next file: ", receivedFile)

                if receivedFile is None:
                    self.mutex.unlock()
                    continue

#                time.sleep(0.2)
                # display image
#                try:
#                    self.subframe.loadFile(receivedFile)
#                # viewer or subframe has been closed by the user
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
            print ("Live view thread: HDF5 not supported yet")

        print ("Live view thread: Thread for Live view died")
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

    def __exit__(self):
        self.query.stop()

    def __del__(self):
        self.query.stop()


if __name__ == '__main__':

    lv = LiveView()

    try:
        print ("LiveViewer start")
        lv.start()

        time.sleep(20)
    finally:
        print ("LiveViewer stop")
        lv.stop()

#    time.sleep(1)
#
#    try:
#        print ("LiveViewer start")
#        lv.start()

#        time.sleep(2)
#    finally:
#        print ("LiveViewer stop")
#        lv.stop()

        del LiveView
