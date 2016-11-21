from __future__ import print_function
from __future__ import unicode_literals
from six import iteritems

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
import sys
import bisect

try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( sys.argv[0] ) ))))
SHARED_PATH  = os.path.join(BASE_PATH, "src", "shared")

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

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
        for event, suffix in iteritems(config["monEvents"]):
            for s in suffix:
                #TODO check format
                patterns.append("*" + s)

        WatchdogEventHandler.patterns = patterns

        self.log.debug("init: super")
        super(WatchdogEventHandler, self,).__init__()

        # learn what events to detect
        self.detect_all    = False
        self.detect_create = False
        self.detect_modify = False
        self.detect_delete = False
        self.detect_move   = False
        self.detect_close  = False

        for event, suffix in iteritems(config["monEvents"]):
            if "all" in event.lower():
                self.log.info("Activate all event types")
                self.detect_all    = tuple(suffix)
            elif "create" in event.lower():
                self.log.info("Activate on create event types")
                self.detect_create = tuple(suffix)
            elif "modify" in event.lower():
                self.log.info("Activate on modify event types")
                self.detect_modify = tuple(suffix)
            elif "delete" in event.lower():
                self.log.info("Activate on delete event types")
                self.detect_delete = tuple(suffix)
            elif "move" in event.lower():
                self.log.info("Activate on move event types")
                self.detect_move   = tuple(suffix)
            elif "close" in event.lower():
                self.log.info("Activate on close event types")
                self.detect_close  = tuple(suffix)

        self.log.debug("self.detect_close={c}, self.detect_move={m}".format(c=self.detect_close, m=self.detect_move))


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Suppress logging messages of watchdog observer
        logging.getLogger("watchdog.observers.inotify_buffer").setLevel(logging.WARNING)

        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("WatchdogEventHandler-{id}".format(id=self.id))
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
        if self.detect_all and event.src_path.endswith(self.detect_all):
            self.log.debug("Any event detected")
            self.process(event)


    def on_created (self, event):
        global eventListToObserve

        if self.detect_create and event.src_path.endswith(self.detect_create):
            #TODO only fire for file-event. skip directory-events.
            self.log.debug("On move event detected")
            self.process(event)
        if self.detect_close and event.src_path.endswith(self.detect_close):
            self.log.debug("On close event detected (from create)")
            self.log.debug("event.src_path={p}".format(p=event.src_path))
            if ( not event.is_directory ):
                self.log.debug("Append event to eventListToObserve: {p}".format(p=event.src_path))
#                eventListToObserve.append(event.src_path)
                bisect.insort_left(eventListToObserve, event.src_path)


    def on_modified (self, event):
        global eventListToObserve

        if self.detect_modify and event.src_path.endswith(self.detect_modify):
            self.log.debug("On modify event detected")
            self.process(event)
        if self.detect_close and event.src_path.endswith(self.detect_close):
            if ( not event.is_directory ) and ( event.src_path not in eventListToObserve ):
                self.log.debug("On close event detected (from modify)")
#                eventListToObserve.append(event.src_path)
                bisect.insort_left(eventListToObserve, event.src_path)


    def on_deleted (self, event):
        if self.detect_delete and event.src_path.endswith(self.detect_delete):
            self.log.debug("On delete event detected")
            self.process(event)


    def on_moved (self, event):
        if self.detect_move and event.src_path.endswith(self.detect_move):
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
    def __init__ (self, NumberOfThreads, timeTillClosed, monDir, actionTime, lock, logQueue):
        self.log = self.getLogger(logQueue)

        self.log.debug("init")
        #Make the Pool of workers
        self.pool           = ThreadPool(NumberOfThreads)
        self.monDir         = monDir
        self.timeTillClosed = timeTillClosed # s
        self.actionTime     = actionTime
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
#                self.log.debug("List to observe: {l}".format(l=eventListToObserve))
#                self.log.debug("eventMessageList: {l}".format(l=eventMessageList))
                if self._poolRunning:
                    self.pool.map(self.checkLastModified, eventListToObserveCopy)
                else:
                    self.log.info("Pool was already closed")
                    break
#                self.log.debug("eventMessageList: {l}".format(l=eventMessageList))

#                self.log.debug("List to observe tmp: {l}".format(l=eventListToObserveTmp))

                self.lock.acquire()
                for event in eventListToObserveTmp:
                    try:
                        eventListToObserve.remove(event)
                        self.log.debug("Removing event: {e}".format(e=event))
                    except:
                        self.log.error("Removing event failed: {e}".format(e=event), exc_info=True)
                        self.log.debug("eventListToObserveTmp={l}".format(l=eventListToObserveTmp))
                        self.log.debug("eventListToObserve={l}".format(l=eventListToObserve))
                eventListToObserveTmp = []
                self.lock.release()

#                self.log.debug("List to observe after map-function: {l}".format(l=eventListToObserve))
                time.sleep(self.actionTime)
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
        except WindowsError:
            self.log.error("Unable to get modification time for file: {p}".format(p=filepath), exc_info=True)
            # remove the file from the observing list
            self.lock.acquire()
            eventListToObserveTmp.append(filepath)
            self.lock.release()
            return
        except:
            self.log.error("Unable to get modification time for file: {p}".format(p=filepath), exc_info=True)
            return

        try:
            # get current time
            timeCurrent = time.time()
        except:
            self.log.error("Unable to get current time for file: {p}".format(p=filepath), exc_info=True)

        # compare ( >= limit)
        if timeCurrent - timeLastModified >= self.timeTillClosed:
            self.log.debug("New closed file detected: {p}".format(p=filepath))

            eventMessage = splitFilePath(filepath, self.monDir)
            self.log.debug("eventMessage: {m}".format(m=eventMessage))

            # add to result list
            self.lock.acquire()
            self.log.debug("checkLastModified-{n} eventMessageList {l}".format(n=threadName, l=eventMessageList))
            eventMessageList.append(eventMessage)
            eventListToObserveTmp.append(filepath)
            self.log.debug("checkLastModified-{n} eventMessageList {l}".format(n=threadName, l=eventMessageList))
#            self.log.debug("checkLastModified-{n} eventListToObserveTmp {l}".format(n=threadName, l=eventListToObserveTmp))
            self.lock.release()
        else:
            self.log.debug("File was last modified {d} sec ago: {p}".format(d=(timeCurrent - timeLastModified), p=filepath))


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
        if ( "monDir" not in config or
                "monSubdirs" not in config or
                "monEvents" not in config or
                "timeTillClosed" not in config or
                "actionTime" not in config ):
            self.log.error ("Configuration of wrong format")
            self.log.debug ("config={c}".format(c=config))
            checkPassed = False
        else:
            checkPassed = True


        if checkPassed:
            self.config          = config
            self.monDir          = self.config["monDir"]
            self.monSubdirs      = self.config["monSubdirs"]

            self.paths           = [os.path.normpath(os.path.join(self.monDir, directory)) for directory in self.config["monSubdirs"]]
            self.log.debug("paths: {p}".format(p=self.paths))

            self.timeTillClosed  = self.config["timeTillClosed"]
            self.actionTime      = self.config["actionTime"]

            self.observerThreads = []
            self.lock            = threading.Lock()


            observerId = 0
            for path in self.paths:
                observer = Observer()
                observer.schedule(WatchdogEventHandler(observerId, self.config, logQueue), path, recursive=True)
                observer.start()
                self.log.info("Started observer for directory: {p}".format(p=path))

                self.observerThreads.append(observer)
                observerId += 1

            self.checkingThread = checkModTime(4, self.timeTillClosed, self.monDir, self.actionTime, self.lock, logQueue)
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
        self.log.info("Stopping observer Threads")
        for observer in  self.observerThreads:
            observer.stop()
            observer.join()

        #close the pool and wait for the work to finish
        self.checkingThread.stop()
        self.checkingThread.join()


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()


if __name__ == '__main__':
    import sys
    from shutil import copyfile
    from subprocess import call
    from multiprocessing import Queue

#    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) ))))
    SHARED_PATH  = os.path.join(BASE_PATH, "src", "shared")
    print (SHARED_PATH)

    if not SHARED_PATH in sys.path:
        sys.path.append ( SHARED_PATH )
    del SHARED_PATH

    import helpers

    logfile  = os.path.join(BASE_PATH, "logs", "watchdogDetector.log")
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
            "monDir"         : os.path.join(BASE_PATH, "data", "source"),
            "monSubdirs"     : ["commissioning", "current", "local"],
            "monEvents"      : {"IN_CLOSE_WRITE" : [".tif", ".cbf"], "IN_MOVED_TO" : [".log"]},
            "timeTillClosed" : 1, #s
            "actionTime"     : 2 #s
            }

    sourceFile = os.path.join(BASE_PATH, "test_file.cbf")
    targetFileBase = os.path.join(BASE_PATH, "data", "source", "local") + os.sep

#    eventDetector = WatchdogDetector(config, logQueue)
    eventDetector = EventDetector(config, logQueue)

    copyFlag = False

    i = 100
    while i <= 105:
        try:
            eventList = eventDetector.getNewEvent()
            if eventList:
                print ("eventList:", eventList)
            if copyFlag:
                targetFile = "{t}{i}.cbf".format(t=targetFileBase, i=i)
                logging.debug("copy to {t}".format(t=targetFile))
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
        targetFile = "{t}{i}.cbf".format(t=targetFileBase, i=number)
        logging.debug("remove {t}".format(t=targetFile))
        os.remove(targetFile)

    logQueue.put_nowait(None)
    logQueueListener.stop()
