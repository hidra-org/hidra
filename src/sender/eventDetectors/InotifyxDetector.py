__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import os
import logging
from inotifyx import binding
from inotifyx.distinfo import version as __version__
import sys
import collections
import threading
import time
import copy


try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( sys.argv[0] ) ))))
SHARED_PATH  = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

from logutils.queue import QueueHandler


constants     = {}
fileEventList = []

for name in dir(binding):
    if name.startswith('IN_'):
        globals()[name] = constants[name] = getattr(binding, name)


# Source: inotifyx library code example
# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
class InotifyEvent (object):
    '''
    InotifyEvent(wd, mask, cookie, name)

    A representation of the inotify_event structure.  See the inotify
    documentation for a description of these fields.
    '''

    wd = None
    mask = None
    cookie = None
    name = None


    def __init__ (self, wd, mask, cookie, name):
        self.wd = wd
        self.mask = mask
        self.cookie = cookie
        self.name = name


    def __str__ (self):
        return '%s: %s' % (self.wd, self.get_mask_description())


    def __repr__ (self):
        return '%s(%s, %s, %s, %s)' % (
          self.__class__.__name__,
          repr(self.wd),
          repr(self.mask),
          repr(self.cookie),
          repr(self.name),
        )


    def get_mask_description (self):
        '''
        Return an ASCII string describing the mask field in terms of
        bitwise-or'd IN_* constants, or 0.  The result is valid Python code
        that could be eval'd to get the value of the mask field.  In other
        words, for a given event:

        >>> from inotifyx import *
        >>> assert (event.mask == eval(event.get_mask_description()))
        '''

        parts = []
        for name, value in constants.items():
            if self.mask & value:
                parts.append(name)
        if parts:
            return '|'.join(parts)
        return '0'


def getEventMessage (path, filename, paths):

    parentDir    = path
    relativePath = ""
    eventMessage = {}

    # traverse the relative path till the original path is reached
    # e.g. created file: /source/dir1/dir2/test.tif
    while True:
        if parentDir not in paths:
            (parentDir,relDir) = os.path.split(parentDir)
            # the os.sep is needed at the beginning because the relative path is built up from the right
            # e.g.
            # self.paths = ["/tmp/test/source"]
            # path = /tmp/test/source/local/testdir
            # first iteration:  parentDir = /tmp/test/source/local, relDir = /testdir
            # second iteration: parentDir = /tmp/test/source,       relDir = /local/testdir
            relativePath = os.sep + relDir + relativePath
        else:
            # remove beginning "/"
            if relativePath.startswith(os.sep):
                relativePath = os.path.normpath(relativePath[1:])
            else:
                relativePath = os.path.normpath(relativePath)

            # the event for a file /tmp/test/source/local/file1.tif is of the form:
            # {
            #   "sourcePath" : "/tmp/test/source"
            #   "relativePath": "local"
            #   "filename"   : "file1.tif"
            # }
            eventMessage = {
                    "sourcePath"  : parentDir,
                    "relativePath": relativePath,
                    "filename"    : filename
                    }

            return eventMessage


class CleanUp (threading.Thread):
    def __init__ (self, paths, monSubdirs, monSuffixes, cleanUpTime, actionTime, lock, logQueue):
        self.log = self.getLogger(logQueue)

        self.log.debug("init")
        self.paths          = paths

        self.monSubdirs     = monSubdirs
        self.monSuffixes    = monSuffixes

        self.cleanUpTime    = cleanUpTime
        self.actionTime     = actionTime

        self.lock           = lock

        self.log.debug("threading.Thread init")
        threading.Thread.__init__(self)


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("CleanUp")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def run (self):
        global fileEventList
        dirsToWalk    = [os.path.normpath(self.paths[0] + os.sep + directory) for directory in self.monSubdirs]

        while True:
            try:
                result = []
                for dirname in dirsToWalk:
                    result += self.traverseDirectory(dirname)

                self.lock.acquire()
                fileEventList += result
                self.lock.release()
#                self.log.debug("fileEventList: " + str(fileEventList))
                time.sleep(self.actionTime)
            except:
                self.log.error("Stopping loop due to error", exc_info=True)
                self.lock.release()
                break


    def traverseDirectory(self, dirname):
        eventList = []

        for root, directories, files in os.walk(dirname):
            for filename in files:
                if not filename.endswith(self.monSuffixes):
#                    self.log.debug("File ending not in monitored Suffixes: " + str(filename))
                    continue

                filepath = os.path.join(root, filename)
                self.log.debug("filepath: " + filepath)

                try:
                    timeLastModified = os.stat(filepath).st_mtime
                except:
                    self.log.error("Unable to get modification time for file: " + filepath, exc_info=True)
                    continue

                try:
                    # get current time
                    timeCurrent = time.time()
                except:
                    self.log.error("Unable to get current time for file: " + filepath, exc_info=True)
                    continue

                if timeCurrent - timeLastModified >= self.cleanUpTime:
                    self.log.debug("New closed file detected: " + str(filepath))
#                    self.log.debug("modTime: " + str(timeLastModified) + "currentTime: " + str(timeCurrent))
#                    self.log.debug("timeCurrent - timeLastModified: " + str(timeCurrent - timeLastModified) + ", cleanUpTime: " + str(self.cleanUpTime))
                    eventMessage = getEventMessage(root, filename, self.paths)
                    self.log.debug("eventMessage: " + str(eventMessage))

                    # add to result list
                    eventList.append(eventMessage)

        return eventList




# Modification of the inotifyx example found inside inotifyx library
# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
class EventDetector():

    def __init__ (self, config, logQueue):

        self.log = self.getLogger(logQueue)

        # check format of config
        if ( not config.has_key("monDir") or
                not config.has_key("monSubdirs") or
                not config.has_key("monEvents") or
                not config.has_key("timeout") or
                not config.has_key("historySize") or
                not config.has_key("useCleanUp") or
                not config.has_key("cleanUpTime") or
                not config.has_key("actionTime") ):
            self.log.error ("Configuration of wrong format")
            self.log.debug ("config="+ str(config))
            checkPassed = False
        else:
            checkPassed = True


        self.wd_to_path   = {}
        self.fd           = binding.init()

        if checkPassed:
            #TODO why is this necessary
            self.paths        = [ config["monDir"] ]

            self.monSubdirs   = config["monSubdirs"]

            suffixList = []
            for key, value in config["monEvents"].iteritems():
                suffixList += value
            self.monSuffixes = tuple(suffixList)

            self.monEvents    = config["monEvents"]

            self.timeout      = config["timeout"]

            self.history      = collections.deque(maxlen=config["historySize"])

            self.cleanUpTime  = config["cleanUpTime"]
            self.actionTime   = config["actionTime"]

            self.lock         = threading.Lock()

            self.add_watch()

            if config["useCleanUp"]:
                self.cleanupThread = CleanUp(self.paths, self.monSubdirs, self.monSuffixes, self.cleanUpTime, self.actionTime, self.lock, logQueue)
                self.cleanupThread.start()


    def get_events (self, fd, *args):
        '''
        get_events(fd[, timeout])

        Return a list of InotifyEvent instances representing events read from
        inotify.  If timeout is None, this will block forever until at least one
        event can be read.  Otherwise, timeout should be an integer or float
        specifying a timeout in seconds.  If get_events times out waiting for
        events, an empty list will be returned.  If timeout is zero, get_events
        will not block.
        '''
        return [
          InotifyEvent(wd, mask, cookie, name)
          for wd, mask, cookie, name in binding.get_events(fd, *args)
        ]


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("InotifyxDetector")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def add_watch (self):
        dirsToRegister=self.getDirectoryStructure()
        try:
            for path in dirsToRegister:
                wd = binding.add_watch(self.fd, path)
                self.wd_to_path[wd] = path
                self.log.debug("Register watch for path:" + str(path) )
        except:
            self.log.error("Could not register watch for path: " + str(path), exc_info=True)


    def getDirectoryStructure (self):
        # Add the default subdirs
        self.log.debug("paths:" + str(self.paths))
        dirsToWalk    = [os.path.normpath(self.paths[0] + os.sep + directory) for directory in self.monSubdirs]
        self.log.debug("dirsToWalk:" + str(dirsToWalk))
        monitoredDirs = []

        # Walk the tree
        for directory in dirsToWalk:
            if os.path.isdir(directory):
                monitoredDirs.append(directory)
                for root, directories, files in os.walk(directory):
                    # Add the found dirs to the list for the inotify-watch
                    monitoredDirs.append(root)
                    self.log.info("Add directory to monitor: " + str(root))
            else:
                self.log.info("Dir does not exist: " + str(directory))

        return monitoredDirs


    def getNewEvent (self):
        global fileEventList

        try:
            self.lock.acquire()
            # get missed files
            eventMessageList = copy.deepcopy(fileEventList)
            fileEventList    = []
        finally:
            self.lock.release()

        if eventMessageList:
            self.log.info("Added missed files: " + str(eventMessageList))

        eventMessage = {}

        events = self.get_events(self.fd, self.timeout)
        removedWd = None

        for event in events:

            if not event.name:
                continue

            try:
                path = self.wd_to_path[event.wd]
            except:
                path = removedWd
            parts = event.get_mask_description()
            parts_array = parts.split("|")

            is_dir        = ("IN_ISDIR" in parts_array)
            is_created    = ("IN_CREATE" in parts_array)
            is_moved_from = ("IN_MOVED_FROM" in parts_array)
            is_moved_to   = ("IN_MOVED_TO" in parts_array)

            currentMonEvent = None
            for key, value in self.monEvents.iteritems():
                if key in parts_array:
                    currentMonEvent = key

#            if not is_dir:
#                self.log.debug(path + " " + event.name + " " + parts)
#            self.log.debug(event.name)
#            self.log.debug("is_dir: " + str(is_dir))
#            self.log.debug("is_created: " + str(is_created))
#            self.log.debug("is_moved_from: " + str(is_moved_from))
#            self.log.debug("is_moved_to: " + str(is_moved_to))


            # if a new directory is created or a directory is renamed inside the monitored one,
            # this one has to be monitored as well
            if is_dir and (is_created or is_moved_to):

#                self.log.debug("is_dir and is created: " + str(is_created) + "
#                self.log.debug(path + " " + event.name + " " + parts)
#                self.log.debug(event.name)

                dirname = os.path.join(path, event.name)
                self.log.info("Directory event detected: " + str(dirname) + "," + str(parts))
                if dirname in self.paths:
                    self.log.debug("Directory already contained in path list: " + str(dirname))
                else:
                    wd = binding.add_watch(self.fd, dirname)
                    self.wd_to_path[wd] = dirname
                    self.log.info("Added new directory to watch:" + str(dirname))

                    # because inotify misses subdirectory creations if they happen to fast,
                    # the newly created directory has to be walked to get catch this misses
                    # http://stackoverflow.com/questions/15806488/inotify-missing-events
                    traversedPath = dirname
                    for root, directories, files in os.walk(dirname):
                        # Add the found dirs to the list for the inotify-watch
                        for dname in directories:
                            traversedPath = os.path.join(traversedPath, dname)
                            wd = binding.add_watch(self.fd, traversedPath)
                            self.wd_to_path[wd] = traversedPath
                            self.log.info("Added new subdirectory to watch:" + str(traversedPath))
                        self.log.debug("files: " +str(files))
                        for filename in files:
#                            self.log.debug("filename: " + filename)
                            if not filename.endswith(self.monSuffixes):
                                self.log.debug("File ending not in monitored Suffixes: " + str(filename))
                                self.log.debug("detected events were: " + str(parts))
                                continue
                            eventMessage = self.getEventMessage(path, filename, self.paths)
                            self.log.debug("eventMessage: " + str(eventMessage))
                            eventMessageList.append(eventMessage)
#                            self.log.debug("eventMessageList: " + str(eventMessageList))
                continue

            # if a directory is renamed the old watch has to be removed
            if is_dir and is_moved_from:

#                self.log.debug("is_dir and is_moved_from")
#                self.log.debug(path + " " + event.name + " " + parts)
#                self.log.debug(event.name)

                dirname = os.path.join(path, event.name)
                for watch, watchPath in self.wd_to_path.iteritems():
                    if watchPath == dirname:
                        foundWatch = watch
                        break
                binding.rm_watch(self.fd, foundWatch)
                self.log.info("Removed directory from watch:" + str(dirname))
                # the IN_MOVE_FROM event always apears before the IN_MOVE_TO (+ additional) events
                # and thus has to be stored till loop is finished
                removedWd = self.wd_to_path[foundWatch]
                # removing the watch out of the dictionary cannot be done inside the loop
                # (would throw error: dictionary changed size during iteration)
                del self.wd_to_path[foundWatch]
                continue

            # only files of the configured event type are send
            if not is_dir and currentMonEvent and [path, event.name] not in self.history:

#                self.log.debug("not is_dir")
#                self.log.debug("currentMonEvent: " + currentMonEvent)
#                self.log.debug(path + " " + event.name + " " + parts)
#                self.log.debug(event.name)

                # only files ending with a suffix specified with the current event are monitored
                if not event.name.endswith(tuple(self.monEvents[currentMonEvent])):
                    self.log.debug("File ending not in monitored Suffixes: " + str(event.name))
                    self.log.debug("detected events were: " + str(parts))
                    continue

                eventMessage = getEventMessage(path, event.name, self.paths)
                self.log.debug("eventMessage" + str(eventMessage))
                eventMessageList.append(eventMessage)

                self.history.append([path, event.name])

        return eventMessageList


    def stop (self):
        try:
            for wd in self.wd_to_path:
                try:
                    binding.rm_watch(self.fd, wd)
                except:
                    self.log.error("Unable to remove watch: " + wd, exc_info=True)
        finally:
            os.close(self.fd)


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


if __name__ == '__main__':
    import sys
    import time
    from subprocess import call
    from multiprocessing import Queue

    import helpers

    logfile  = BASE_PATH + os.sep + "logs" + os.sep + "inotifyDetector.log"
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
            "eventDetectorType" : "inotifyx",
            "monDir"            : BASE_PATH + os.sep + "data" + os.sep + "source",
            "monSubdirs"        : ["commissioning", "current", "local"],
            "monitoredEvents"   : {"IN_CLOSE_WRITE" : [".tif", ".cbf"], "IN_MOVED_TO" : [".log"]},
            "timeout"           : 0.1,
            "historySize"       : 0,
            "useCleanUp"        : False,
            "cleanUpTime"       : 5,
            "actionTime"        : 120
            }

    eventDetector = EventDetector(config, logQueue)

    sourceFile = BASE_PATH + os.sep + "test_file.cbf"
    targetFileBase = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep

    i = 100
    while i <= 110:
        try:
            logging.debug("copy")
            targetFile = targetFileBase + str(i) + ".cbf"
            call(["cp", sourceFile, targetFile])
#            copyfile(sourceFile, targetFile)
            i += 1

            eventList = eventDetector.getNewEvent()
            if eventList:
                print "eventList:", eventList

            time.sleep(1)
        except KeyboardInterrupt:
            break

    for number in range(100, i):
        targetFile = targetFileBase + str(number) + ".cbf"
        logging.debug("remove " + targetFile)
        os.remove(targetFile)

    logQueue.put_nowait(None)
    logQueueListener.stop()
