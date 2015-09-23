__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import os
import logging
from inotifyx import binding
from inotifyx.distinfo import version as __version__

import helperScript

constants = {}

for name in dir(binding):
    if name.startswith('IN_'):
        globals()[name] = constants[name] = getattr(binding, name)


# Source: inotifyx library code example
# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
class InotifyEvent(object):
    '''
    InotifyEvent(wd, mask, cookie, name)

    A representation of the inotify_event structure.  See the inotify
    documentation for a description of these fields.
    '''

    wd = None
    mask = None
    cookie = None
    name = None

    def __init__(self, wd, mask, cookie, name):
        self.wd = wd
        self.mask = mask
        self.cookie = cookie
        self.name = name

    def __str__(self):
        return '%s: %s' % (self.wd, self.get_mask_description())

    def __repr__(self):
        return '%s(%s, %s, %s, %s)' % (
          self.__class__.__name__,
          repr(self.wd),
          repr(self.mask),
          repr(self.cookie),
          repr(self.name),
        )

    def get_mask_description(self):
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


# Modification of the inotifyx example found inside inotifyx library
# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
class InotifyDetector():
    paths      = []
    wd_to_path = {}
    fd         = None
    log        = None


    def __init__(self, paths, monitoredSubfolders, monitoredSuffixes):

        self.paths = paths
        self.log  = self.getLogger()
        self.fd  = binding.init()
        self.monitoredSuffixes   = monitoredSuffixes
        self.monitoredSubfolders = monitoredSubfolders

        self.add_watch()


    def get_events(self, fd, *args):
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


    def getLogger(self):
        logger = logging.getLogger("inotifyDetector")
        return logger


    def add_watch(self):
        foldersToRegister=self.getDirectoryStructure()
        try:
#            for path in self.paths:
            for path in foldersToRegister:
                wd = binding.add_watch(self.fd, path)
                self.wd_to_path[wd] = path
                self.log.debug("Register watch for path:" + str(path) )
        except Exception as e:
            self.log.error("Could not register watch for path: " + str(path) )
            self.log.debug("Error was " + str(e))
            self.stop()


    def getDirectoryStructure(self):
        # Add the default subfolders
        self.log.info("paths:" + str(self.paths))
        foldersToWalk    = [self.paths[0] + os.sep + folder for folder in self.monitoredSubfolders]
        self.log.info("foldersToWalk:" + str(foldersToWalk))
        monitoredFolders = []

        # Walk the tree
        for folder in foldersToWalk:
            for root, directories, files in os.walk(folder):
                # Add the found folders to the list for the inotify-watch
                monitoredFolders.append(root)
                self.log.info("Add folder to monitor: " + str(root))

        return monitoredFolders

    def getNewEvent(self):

        eventMessageList = []
        eventMessage = {}

#        print "wd_to_path: ", self.wd_to_path
        events = self.get_events(self.fd)
        for event in events:
            path = self.wd_to_path[event.wd]
            parts = event.get_mask_description()
            parts_array = parts.split("|")

            if not event.name:
                return []

#            print path, event.name, parts
#            print event.name

            is_dir     = ("IN_ISDIR" in parts_array)
            is_closed  = ("IN_CLOSE_WRITE" in parts_array)
            is_moved   = ("IN_MOVE" in parts_array)
#            is_closed  = ("IN_CLOSE" in parts_array)
#            is_closed  = ("IN_CLOSE" in parts_array or "IN_CLOSE_WRITE" in parts_array)
            is_created = ("IN_CREATE" in parts_array)

            # if a new directory is created inside the monitored one,
            # this one has to be monitored as well
#            print path, event.name, parts

            if is_created and is_dir and event.name:
#            if is_dir and event.name:
                dirname =  path + os.sep + event.name
                self.log.info("Directory event detected: " + str(dirname) + "," + str(parts))
                if dirname in self.paths:
                    self.log.debug("Directory already contained in path list: " + str(dirname))
                else:
                    wd = binding.add_watch(self.fd, dirname)
                    self.wd_to_path[wd] = dirname
                    self.log.info("Added new directory to watch:" + str(dirname))

#            if not event.name.endswith(self.monitoredSuffixes):
#                print "not considered", event.name
#                return []

            if '.cbf' not in event.name :
                self.log.debug("not a cbf-file: " + str(event.name))
                self.log.debug("detected events were: " + str(parts))
                return []

            # only closed files are send
            if is_closed and not is_dir:
#            if (is_moved and not is_dir) or (is_closed and not is_dir):
#                print path, event.name, parts
                if event.name[0] == '.' :
                    self.log.debug("Removing '.' and suffix from event name: " + str(event.name))
                    event_name_dirty_hack = event.name.rsplit(".", 1)[0][1:]
                else :
                    self.log.debug("Correct eevent name format: " + str(event.name))
                    event_name_dirty_hack = event.name
                parentDir    = path
                relativePath = ""
                eventMessage = {}

                # traverse the relative path till the original path is reached
                # e.g. created file: /source/dir1/dir2/test.tif
                while True:
                    if parentDir not in self.paths:
                        (parentDir,relDir) = os.path.split(parentDir)
#                        print "debug1:", parentDir, relDir
                        relativePath = os.sep + relDir + relativePath
#                        print "debug11:", relativePath
                    else:
                        # the event for a file /tmp/test/source/local/file1.tif is of the form:
                        # {
                        #   "sourcePath" : "/tmp/test/source/"
                        #   "relativePath": "local"
                        #   "filename"   : "file1.tif"
                        # }
                        eventMessage = {
                                "sourcePath"  : parentDir,
                                "relativePath": relativePath,
                                "filename"    : event_name_dirty_hack
                                }
#                        print "eventMessage:", eventMessage
                        eventMessageList.append(eventMessage)
                        break

        return eventMessageList



    def process(self):
        try:
            try:
                while True:
                    self.getNewEvent()
            except KeyboardInterrupt:
                pass
        finally:
            self.stop()


    def stop(self):

        try:
            try:
                for wd in self.wd_to_path:
                    binding.rm_watch(self.fd, wd)
            except Exception as e:
                    self.log.error("Unable to remove watch.")
                    self.log.debug("Error was: " + str(e))
        finally:
            os.close(self.fd)




if __name__ == '__main__':
    logfilePath = "/space/projects/live-viewer/logs/inotifyDetector.log"
    verbose=True

    #enable logging
    helperScript.initLogging(logfilePath, verbose)

    monitoredFolders = ["/space/projects/live-viewer/data/source/local"]
    monitoredSuffixes = (".tif", ".cbf")

    eventDetector = InotifyDetector(monitoredFolders, monitoredSuffixes)

    while True:
        try:
            eventList = eventDetector.getNewEvent()
            if eventList:
                print eventList
        except KeyboardInterrupt:
            break
