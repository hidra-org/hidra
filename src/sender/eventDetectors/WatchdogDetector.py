__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import os
import logging

import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import sys
import copy
from multiprocessing.dummy import Pool as ThreadPool
import threading
from logutils.queue import QueueHandler


eventMessageList = []
eventListToObserve = []
eventListToObserveTmp = []


class WatchdogEventHandler (PatternMatchingEventHandler):
    def __init__ (self, id, config, logQueue):
        self.id = id
        self.log = self.getLogger(logQueue)

        self.log.debug("init")

        self.paths        = [ config["monDir"] ]

        patterns = []
        for suffix in config["monSuffixes"]:
            #TODO check format
            patterns.append("*" + suffix)

        WatchdogEventHandler.patterns = patterns

        self.log.debug("init: super")
#        PatternMatchingEventHandler.__init__()
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
            self.detect_close  = True


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("WatchdogEventHandler-" + str(self.id))
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def process (self, event):
        self.log.debug("process")

        global eventMessageList

        # Directories will be skipped
        if not event.is_directory:

            eventMessage = splitFilePath(event.src_path, self.paths)

            eventMessageList.append(eventMessage)


    def on_any_event (self, event):
        if self.detect_all:
            self.log.debug("Any event detected")
            self.process(event)


    def on_created (self, event):
        global eventListToObserve

        if self.detect_create:
            #TODO only fire for file-event. skip directory-events.
            self.log.debug("On move event detected")
            self.process(event)
        if self.detect_close:
            self.log.debug("On close event detected (from create)")
            if ( not event.is_directory ):
                self.log.debug("Append event to eventListToObserve: " + event.src_path)
                eventListToObserve.append(event.src_path)


    def on_modified (self, event):
        global eventListToObserve

        if self.detect_modify:
            self.log.debug("On modify event detected")
            self.process(event)
        if self.detect_close:
            if ( not event.is_directory ) and ( event.src_path not in eventListToObserve ):
                self.log.debug("On close event detected (from modify)")
                eventListToObserve.append(event.src_path)


    def on_deleted (self, event):
        if self.detect_delete:
            self.log.debug("On delete event detected")
            self.process(event)


    def on_moved (self, event):
        if self.detect_move:
            self.log.debug("On move event detected")
            self.process(event)


def splitFilePath (filepath, paths):

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
            if relativePath:
                relativePath = os.path.join(relDir, relativePath)
                #relativePath = os.sep + relDir + relativePath
            else:
                relativePath = relDir



#    commonPrefix         = os.path.commonprefix([self.monDir,filepath]) # corresponds to sourcePath
#    relativeBasepath     = os.path.relpath(filepath, commonPrefix)      # corresponds to relativePath + filename
#    (relativeParent, filename_tmp) = os.path.split(relativeBasepath)    # corresponds to relativePath

    # the event for a file /tmp/test/source/local/file1.tif is of the form:
    # {
    #   "sourcePath" : "/tmp/test/source"
    #   "relativePath": "local"
    #   "filename"   : "file1.tif"
    # }
    eventMessage = {
            "sourcePath"  : os.path.normpath(parentDir),
            "relativePath": os.path.normpath(relativePath),
            "filename"    : filename
            }

    return eventMessage


class checkModTime (threading.Thread):
    def __init__ (self, NumberOfThreads, timeTillClosed, monDir, lock, logQueue):
        self.log = self.getLogger(logQueue)

        self.log.debug("init")
        #Make the Pool of workers
        self.pool           = ThreadPool(NumberOfThreads)
        self.monDir         = monDir
        self.timeTillClosed = timeTillClosed # s
        self.lock           = lock
        self._stop          = threading.Event()
        self._poolRunning   = True

        self.log.debug("threading.Thread init")
        threading.Thread.__init__(self)


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("checkModTime")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def run (self):
        global eventListToObserve
        global eventListToObserveTmp

        while True:
            try:
                self.lock.acquire()
                eventListToObserveCopy = copy.deepcopy(eventListToObserve)
                self.lock.release()

                # Open the urls in their own threads
#                self.log.debug("List to observe: " + str(eventListToObserve))
#                self.log.debug("eventMessageList: " + str(eventMessageList))
                if self._poolRunning:
                    self.pool.map(self.checkLastModified, eventListToObserveCopy)
                else:
                    self.log.debug("Pool was already closed")
                    break
#                self.log.debug("eventMessageList: " + str(eventMessageList))

#                self.log.debug("List to observe tmp: " + str(eventListToObserveTmp))

                self.lock.acquire()
                for event in eventListToObserveTmp:
                    try:
                        eventListToObserve.remove(event)
                        self.log.debug("Removing event: " + event)
                    except:
                        self.log.error("Removing event failed: " + event, exc_info=True)
                        self.log.debug("eventListToObserveTmp=" +str(eventListToObserveTmp))
                        self.log.debug("eventListToObserve=" +str(eventListToObserve))
                eventListToObserveTmp = []
                self.lock.release()

#                self.log.debug("List to observe after map-function: " + str(eventListToObserve))
                time.sleep(2)
            except:
                self.log.error("Stopping loop due to error", exc_info=True)
                break


    def checkLastModified (self, filepath):
        global eventMessageList
        global eventListToObserveTmp

        threadName = threading.current_thread().name

        try:
            # check modification time
            timeLastModified = os.stat(filepath).st_mtime
        except:
            self.log.error("Unable to get modification time for file: " + filepath, exc_info=True)
            return

        try:
            # get current time
            timeCurrent = time.time()
        except:
            self.log.error("Unable to get current time for file: " + filepath, exc_info=True)

        # compare ( >= limit)
        if timeCurrent - timeLastModified >= self.timeTillClosed:
            self.log.debug("New closed file detected: " + str(filepath))

            eventMessage = splitFilePath(filepath, self.monDir)
            self.log.debug("eventMessage: " + str(eventMessage))

            # add to result list
            self.lock.acquire()
            self.log.debug("checkLastModified-" + str(threadName) + " eventMessageList" + str(eventMessageList))
            eventMessageList.append(eventMessage)
            eventListToObserveTmp.append(filepath)
            self.log.debug("checkLastModified-" + str(threadName) + " eventMessageList" + str(eventMessageList))
#            self.log.debug("checkLastModified-" + str(threadName) + " eventListToObserveTmp" + str(eventListToObserveTmp))
            self.lock.release()
        else:
            self.log.debug("File was last modified " + str(timeCurrent - timeLastModified) + \
                           " sec ago: " + str(filepath))


    def stop (self):
        #close the pool and wait for the work to finish
        self.pool.close()
        self.pool.join()
        self._stop.set()
        self._poolRunning = False


    def stopped (self):
        return self._stop.isSet()


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


#class WatchdogDetector():
class EventDetector():
    def __init__(self, config, logQueue):

        self.log = self.getLogger(logQueue)

        # check format of config
        if ( not config.has_key("monDir") or
                not config.has_key("monEventType") or
                not config.has_key("monSubdirs") or
                not config.has_key("monSuffixes") or
                not config.has_key("timeTillClosed") ):
            self.log.error ("Configuration of wrong format")
            self.log.debug ("config="+ str(config))
            checkPassed = False
        else:
            checkPassed = True

        self.log.debug("init")

        if checkPassed:
            self.config          = config
            self.monDir          = self.config["monDir"]
            self.monSubdirs      = self.config["monSubdirs"]
            self.log.info("monDir: " + str(self.monDir))

            self.paths           = [os.path.normpath(self.monDir + os.sep + directory) for directory in self.config["monSubdirs"]]
            self.log.info("paths: " + str(self.paths))

            self.timeTillClosed  = self.config["timeTillClosed"]
            self.observerThreads = []
            self.lock            = threading.Lock()


            observerId = 0
            for path in self.paths:
                observer = Observer()
                observer.schedule(WatchdogEventHandler(observerId, self.config, logQueue), path, recursive=True)
                observer.start()
                self.log.info("Started observer for directory: " + path)

                self.observerThreads.append(observer)
                observerId += 1

            self.checkingThread = checkModTime(4, self.timeTillClosed, self.monDir, self.lock, logQueue)
            self.checkingThread.start()


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("WatchdogDetector")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def getNewEvent(self):
        global eventMessageList

        self.lock.acquire()
        eventMessageListlocal = copy.deepcopy(eventMessageList)
        # reset global list
        eventMessageList = []
        self.lock.release()
        return eventMessageListlocal


    def stop(self):
        for observer in  self.observerThreads:
            observer.stop()
            observer.join()

        #close the pool and wait for the work to finish
        self.checkingThread.stop()
        self.checkingThread.join()


    def __exit__(self):
        self.stop()


if __name__ == '__main__':
    import sys
    from shutil import copyfile
    from subprocess import call
    from multiprocessing import Queue

#    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) ))))
    SHARED_PATH  = BASE_PATH + os.sep + "src" + os.sep + "shared"
    print SHARED_PATH

    if not SHARED_PATH in sys.path:
        sys.path.append ( SHARED_PATH )
    del SHARED_PATH

    import helpers

    logfile  = BASE_PATH + os.sep + "logs" + os.sep + "watchdogDetector.log"
    logsize  = 10485760

    logQueue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose=True, onScreenLogLevel="debug")

    # Start queue listener using the stream handler above
    logQueueListener = helpers.CustomQueueListener(logQueue, h1, h2)
    logQueueListener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG) # Log level = DEBUG
    qh = QueueHandler(logQueue)
    root.addHandler(qh)


    config = {
            #TODO normpath to make insensitive to "/" at the end
            "monDir"         : BASE_PATH + os.sep + "data" + os.sep + "source",
            "monEventType"   : "ON_CLOSE",
#            "monEventType"   : "IN_CREATE",
            "monSubdirs"     : ["commissioning", "current", "local"],
            "monSuffixes"    : [".tif", ".cbf"],
            "timeTillClosed" : 1 #s
            }

    sourceFile = BASE_PATH + os.sep + "test_file.cbf"
    targetFileBase = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep

#    eventDetector = WatchdogDetector(config, logQueue)
    eventDetector = EventDetector(config, logQueue)

    copyFlag = False

    i = 100
    while i <= 105:
        try:
            eventList = eventDetector.getNewEvent()
            if eventList:
                print "eventList:", eventList
            if copyFlag:
                targetFile = targetFileBase + str(i) + ".cbf"
                logging.debug("copy to " + targetFile)
#                call(["cp", sourceFile, targetFile])
                copyfile(sourceFile, targetFile)
                i += 1
                copyFlag = False
            else:
                copyFlag = True

            time.sleep(0.5)
        except KeyboardInterrupt:
            break

    time.sleep(2)
    eventDetector.stop()
    for number in range(100, i):
        targetFile = targetFileBase + str(number) + ".cbf"
        logging.debug("remove " + targetFile)
        os.remove(targetFile)

    logQueue.put_nowait(None)
    logQueueListener.stop()
