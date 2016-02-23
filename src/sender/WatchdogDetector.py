__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import os
import logging
#from inotifyx.distinfo import version as __version__

import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import sys
import copy


eventMessageList = []


class WatchdogEventHandler(PatternMatchingEventHandler):
    patterns = ['*.tif', '*.cbf']

    def __init__(self, config):
        self.log = self.getLogger()
        self.log.debug("init")
        # TODO multiple monDirs
        self.monDir       = config["monDir"][0]
        self.paths        = config["monDir"]
        self.monSubdirs   = config["monSubdirs"]
        patterns = []
        for suffix in config["monSuffixes"]:
            #TODO check format
            patterns.append("*" + suffix)

        print "patterns", patterns

#        self.patterns = patterns

        self.log.debug("init: super")
        super(WatchdogEventHandler, self,).__init__()

        self.detect_all    = False
        self.detect_create = False
        self.detect_modify = False
        self.detect_delete = False
        self.detect_move   = False

        if "all" in config["monEventType"].lower():
            self.log.debug("Activate all event types")
            self.detect_all    = True
        elif "create" in config["monEventType"].lower():
            self.log.debug("Activate on create event types")
            self.detect_create = True
        elif "modify" in config["monEventType"].lower():
            self.log.debug("Activate on modify event types")
            self.detect_modify = True
        elif "delete" in config["monEventType"].lower():
            self.log.debug("Activate on delete event types")
            self.detect_delete = True
        elif "move" in config["monEventType"].lower():
            self.log.debug("Activate on move event types")
            self.detect_move   = True


    def getLogger(self):
        logger = logging.getLogger("WatchdogEventHandler")
        return logger


    def process(self, event):
        self.log.debug("process")

        global eventMessageList

        #only passes files-events to zeromq. directories will be skipped
        if not event.is_directory:

            filepath = event.src_path

            (parentDir,filename) = os.path.split(filepath)
            relativePath = ""
            eventMessage = {}


            #extract relative pathname and filename for the file.
            while True:
                if parentDir in self.paths:
                    break
                else:
                    (parentDir,relDir) = os.path.split(parentDir)
                    # the os.sep is needed at the beginning because the relative path is built up from the right
                    # e.g.
                    # self.paths = ["/tmp/test/source"]
                    # path = /tmp/test/source/local/testdir
                    # first iteration: self.monEventType parentDir = /tmp/test/source/local, relDir = /testdir
                    # second iteration: parentDir = /tmp/test/source,       relDir = /local/testdir
                    relativePath = os.sep + relDir + relativePath


#            commonPrefix         = os.path.commonprefix([self.monDir,filepath]) # corresponds to sourcePath
#            relativeBasepath     = os.path.relpath(filepath, commonPrefix)      # corresponds to relativePath + filename
#            (relativeParent, filename_tmp) = os.path.split(relativeBasepath)    # corresponds to relativePath

            # the event for a file /tmp/test/source/local/file1.tif is of the form:
            # {
            #   "sourcePath" : "/tmp/test/source"
            #   "relativePath": "/local"
            #   "filename"   : "file1.tif"
            # }
            eventMessage = {
                    "sourcePath"  : parentDir,
                    "relativePath": relativePath,
                    "filename"    : filename
                    }
            print "eventMessage", eventMessage


            eventMessageList.append(eventMessage)


    def on_any_event(self, event):
        if self.detect_all:
            self.log.debug("Any event detected")
            self.process(event)


    def on_created(self, event):
        if self.detect_create:
            #TODO does an on_close exists? otherwise need to implement a wait-periode before processing the event/message
            #@see http://stackoverflow.com/questions/22406309/how-to-get-a-file-close-event-in-python

            #TODO only fire for file-event. skip directory-events.
            self.log.debug("On move event detected")
            self.process(event)


    def on_modified(self, event):
        if self.detect_modify:
            self.log.debug("On modify event detected")
            self.process(event)


    def on_deleted(self, event):
        if self.detect_delete:
            self.log.debug("On delete event detected")
            self.process(event)


    def on_moved(self, event):
        if self.detect_move:
            self.log.debug("On move event detected")
            self.process(event)





class WatchdogDetector():

    def __init__(self, config):
        self.log = self.getLogger()

        self.log.debug("init")

        self.config = config
        self.monDir = self.config["monDir"][0]

        self.observer = Observer()
        self.observer.schedule(WatchdogEventHandler(self.config), path=self.monDir, recursive=True)
        self.observer.start()


    def getLogger(self):
        logger = logging.getLogger("WatchdogDetector")
        return logger


    def getNewEvent(self):
        global eventMessageList

        eventMessageListlocal = copy.deepcopy(eventMessageList)
        # reset global list
        eventMessageList = []
        return eventMessageListlocal


    def stop(self):
        self.observer.stop()
        self.observer.join()


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()


if __name__ == '__main__':
    import sys
    from shutil import copyfile
    from subprocess import call

    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
    SRC_PATH  = BASE_PATH + os.sep + "src"

    sys.path.append ( SRC_PATH )

    import shared.helperScript as helperScript

    logfilePath = BASE_PATH + "/logs/watchdogDetector.log"
    verbose     = True
    onScreen    = "debug"

    #enable logging
    helperScript.initLogging(logfilePath, verbose, onScreen)

    config = {
            #TODO normpath to make insensitive to "/" at the end
            "monDir"       : [ BASE_PATH + "/data/source" ],
            "monEventType" : "IN_CREATE",
            "monSubdirs"   : ["local"],
            "monSuffixes"  : [".tif", ".cbf"]
            }

    sourceFile = BASE_PATH + "/test_file.cbf"
    targetFile = BASE_PATH + "/data/source/local/raw/100.cbf"

    eventDetector = WatchdogDetector(config)

    copyFlag = False

    while True:
        try:
            eventList = eventDetector.getNewEvent()
            if eventList:
                print eventList
            if copyFlag:
                logging.debug("copy")
                call(["cp", sourceFile, targetFile])
#                copyfile(sourceFile, targetFile)
                logging.debug("remove")
                os.remove(targetFile)
                copyFlag = False
            else:
                copyFlag = True

            time.sleep(2)
        except KeyboardInterrupt:
            break

