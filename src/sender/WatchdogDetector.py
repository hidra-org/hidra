__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import os
import logging
#from inotifyx.distinfo import version as __version__

import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import sys
import copy
from multiprocessing.dummy import Pool as ThreadPool
import threading


eventMessageList = []
eventListToObserve = []


class WatchdogEventHandler(PatternMatchingEventHandler):
    patterns = ['*.tif', '*.cbf']

    def __init__(self, config):
        self.log = self.getLogger()
        self.log.debug("init")
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

        # learn what events to detect
        self.detect_all    = False
        self.detect_create = False
        self.detect_modify = False
        self.detect_delete = False
        self.detect_move   = False
        self.detect_close  = False

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
        elif "close" in config["monEventType"].lower():
            self.log.debug("Activate on close event types")
            self.detect_close   = True


    def getLogger(self):
        logger = logging.getLogger("WatchdogEventHandler")
        return logger


    def process(self, event):
        self.log.debug("process")

        global eventMessageList

        # Directories will be skipped
        if not event.is_directory:

            eventMessage = splitFilePath(event.src_path, self.paths)

            eventMessageList.append(eventMessage)


    def on_any_event(self, event):
        if self.detect_all:
            self.log.debug("Any event detected")
            self.process(event)


    def on_created(self, event):
        global eventListToObserve

        if self.detect_create:
            #TODO only fire for file-event. skip directory-events.
            self.log.debug("On move event detected")
            self.process(event)
        if self.detect_close:
            self.log.debug("On close event detected")
            if ( not event.is_directory ):
                self.log.debug("Append event to eventListToObserve")
                eventListToObserve.append(event.src_path)


    def on_modified(self, event):
        global eventListToObserve

        if self.detect_modify:
            self.log.debug("On modify event detected")
            self.process(event)
        if self.detect_close and False:
            self.log.debug("On close event detected")
            if ( not event.is_directory ) and ( event.src_path not in eventListToObserve ):
                eventListToObserve.append(event.src_path)


    def on_deleted(self, event):
        if self.detect_delete:
            self.log.debug("On delete event detected")
            self.process(event)


    def on_moved(self, event):
        if self.detect_move:
            self.log.debug("On move event detected")
            self.process(event)


def splitFilePath(filepath, paths):

    (parentDir,filename) = os.path.split(filepath)
    relativePath = ""
    eventMessage = {}

    #extract relative pathname and filename for the file.
    while True:
        if parentDir in paths:
            break
        else:
            (parentDir,relDir) = os.path.split(parentDir)
            # the os.sep is needed at the beginning because the relative path is built up from the right
            # e.g.
            # self.paths = ["/tmp/test/source"]
            # path = /tmp/test/source/local/testdir
            # first iteration:  parentDir = /tmp/test/source/local, relDir = /testdir
            # second iteration: parentDir = /tmp/test/source,       relDir = /local/testdir
            relativePath = os.sep + relDir + relativePath


#    commonPrefix         = os.path.commonprefix([self.monDir,filepath]) # corresponds to sourcePath
#    relativeBasepath     = os.path.relpath(filepath, commonPrefix)      # corresponds to relativePath + filename
#    (relativeParent, filename_tmp) = os.path.split(relativeBasepath)    # corresponds to relativePath

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

    return eventMessage


class checkModTime(threading.Thread):
    def __init__(self, NumberOfThreads, paths):
        self.log = self.getLogger()

        self.log.debug("init")
        #Make the Pool of workers
        self.pool  = ThreadPool(NumberOfThreads)
        self.paths = paths
        self._stop = threading.Event()

        self.log.debug("threading.Thread init")
        threading.Thread.__init__(self)


    def getLogger(self):
        logger = logging.getLogger("checkModTime")
        return logger


    def run(self):
        global eventListToObserve

        while True:
            try:
                # Open the urls in their own threads
                self.log.debug("loop: " + str(eventListToObserve))
                self.log.debug("eventMessageList: " + str(eventMessageList))
                self.pool.map(self.checkLastModified, eventListToObserve)
                self.log.debug("eventMessageList: " + str(eventMessageList))
                time.sleep(2)
            except:
                break


    def checkLastModified(self, filepath):
        global eventMessageList

        try:
            # check modification time
            timeLastModified = os.stat(filepath).st_mtime
        except Exception as e:
            self.log.error("Unable to get modification time for file: " + filepath)
            self.log.error("Error was: " + str(e))
            return

        self.log.debug("modification Time: " + str(timeLastModified))

        try:
            # get current time
            timeCurrent = time.time()
        except Exception as e:
            self.log.error("Unable to get current time for file: " + filepath)
            self.log.error("Error was: " + str(e))

        self.log("current Time: " + str(timeCurrent))
        # compare ( >= limit)
        if timeCurrent - timeLastModified >= timeToWait:

            eventMessage = splitFilePath(filepath, self.paths)

            # add to result list
            eventMessageList.append(eventMessage)


    def stop(self):
        #close the pool and wait for the work to finish
        self.pool.close()
        self.pool.join()
        self._stop.set()


    def stopped(self):
        return self._stop.isSet()


    def __exit__(self):
        self.stop()


class WatchdogDetector():

    def __init__(self, config):
        self.log = self.getLogger()

        self.log.debug("init")

        self.config = config
        self.paths  = self.config["monDir"]
        self.monDir = self.config["monDir"][0]

        self.observer = Observer()
        self.observer.schedule(WatchdogEventHandler(self.config), path=self.monDir, recursive=True)
        self.observer.start()

        self.checkingThread = checkModTime(4, self.paths)
        self.checkingThread.start()


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

        #close the pool and wait for the work to finish
        self.checkingThread.stop()
        self.checkingThread.join()


    def __exit__(self):
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
            "monEventType" : "ON_CLOSE",
#            "monEventType" : "IN_CREATE",
            "monSubdirs"   : ["local"],
            "monSuffixes"  : [".tif", ".cbf"]
            }

    sourceFile = BASE_PATH + "/test_file.cbf"
    targetFileBase = BASE_PATH + "/data/source/local/raw/"

    eventDetector = WatchdogDetector(config)

    copyFlag = False

    i = 100
    while True:
        try:
            eventList = eventDetector.getNewEvent()
            if eventList:
                print eventList
            if copyFlag:
                logging.debug("copy")
                targetFile = targetFileBase + str(i) + ".cbf"
                call(["cp", sourceFile, targetFile])
                i += 1
#                copyfile(sourceFile, targetFile)
                copyFlag = False
            else:
                copyFlag = True

            time.sleep(2)
        except KeyboardInterrupt:
            break

    eventDetector.stop()
    for number in range(100, i):
        targetFile = targetFileBase + str(number) + ".cbf"
        logging.debug("remove " + targetFile)
        os.remove(targetFile)
