__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import time
import logging
import os
from stat import S_ISREG, ST_MTIME, ST_MODE
import helperScript


class RingBuffer:
    targetDir         = None
    ringBuffer        = []
    maxRingBufferSize = None
    storeContent      = None

    log               = None

    def __init__(self, maxRingBufferSize, storeContent = False, targetDir = None):
        self.targetDir          = targetDir
        self.maxRingBufferSize  = maxRingBufferSize
        self.storeContent       = storeContent

        self.log = self.getLogger()
        self.log.debug("Init")

        # initialize ring buffer
        # get all entries in the directory
        if targetDir:
            # TODO empty target dir -> ringBuffer = []
            self.ringBuffer = (os.path.join(self.targetDir, fn) for fn in os.listdir(self.targetDir))
            # get the corresponding stats
            self.ringBuffer = ((os.stat(path), path) for path in self.ringBuffer)
            # leave only regular files, insert modification date
            self.ringBuffer = [[stat[ST_MTIME], path]
                    for stat, path in self.ringBuffer if S_ISREG(stat[ST_MODE])]

            # sort the ring buffer in descending order (new to old files)
            self.ringBuffer = sorted(self.ringBuffer, reverse=True)
            self.log.debug("Init ring buffer with files in specified directory")


    def getLogger(self):
        logger = logging.getLogger("RingBuffer")
        return logger


    def __str__(self):
        returnString = ""

        if self.ringBuffer:
            for i in self.ringBuffer[:-1]:
                returnString += str(i) + "\n"
            returnString += str(self.ringBuffer[-1])

        return returnString


    def getNewestFile(self):
        # send first element in ring buffer to live viewer (the path of this file is the second entry)
        if self.storeContent:
            if self.ringBuffer:
                self.log.debug("Newest Event returned")
                return self.ringBuffer[0][2]
            else:
                self.log.debug("Newest Event: None")
                return "None"
        else:
            if self.ringBuffer:
                self.log.debug("Newest Event: " + str(self.ringBuffer[0][1]) )
                return self.ringBuffer[0][1]
            else:
                self.log.debug("Newest Event: None")
                return "None"


    def popNewestFile(self):
        # send first element in ring buffer to live viewer (the path of this file is the second entry)
        if self.storeContent:
            if self.ringBuffer:
                elementModTime, elementPath, elementContent = self.ringBuffer[0]
                try:
#                    os.remove(elementPath)
                    self.ringBuffer.remove([elementModTime, elementPath, elementContent])
                    self.log.debug("Newest Event removed")
                except Exception as e:
                    self.log.debug("Newest Event removing failed: " + str(elementPath))
                    self.log.debug("Error was: " + str(e))

                self.log.debug("Newest Event returned")
                return elementContent
            else:
                self.log.debug("Newest Event: None")
                return "None"
        else:
            if self.ringBuffer:
                self.log.debug("Newest Event: " + str(self.ringBuffer[0][1]) )
                return self.ringBuffer[0][1]
            else:
                self.log.debug("Newest Event: None")
                return "None"


    def add(self, filename, fileModTime, fileContent=False):
        if fileContent and self.storeContent:
            # prepend file to ring buffer and restore order
            self.ringBuffer[:0] = [[fileModTime, filename, fileContent]]
            self.ringBuffer = sorted(self.ringBuffer, reverse=True)

            # if the maximal size is exceeded: remove the oldest files
            if len(self.ringBuffer) > self.maxRingBufferSize:
                for mod_time, path, content in self.ringBuffer[self.maxRingBufferSize:]:
                    self.log.debug("Remove file from ring buffer: " + str(path) )
#                    os.remove(path)
                    self.ringBuffer.remove([mod_time, path, content])
        else:
            # prepend file to ring buffer and restore order
            self.ringBuffer[:0] = [[fileModTime, filename]]
            self.ringBuffer = sorted(self.ringBuffer, reverse=True)

            # if the maximal size is exceeded: remove the oldest files
            if len(self.ringBuffer) > self.maxRingBufferSize:
                for mod_time, path in self.ringBuffer[self.maxRingBufferSize:]:
                    self.log.debug("Remove file from ring buffer: " + str(path) )
#                    os.remove(path)
                    self.ringBuffer.remove([mod_time, path])

    def removeAll(self):
        self.log.debug("Remove all elements in ring buffer and from disc")

        for i in range(len(self.ringBuffer)):
            filePath = self.ringBuffer[i][1]
            self.log.debug("Remove file from disk: " + str(filePath) )
            os.remove(filePath)
        self.log.debug("Clear ring buffer")
        self.ringBuffer = []




if __name__ == "__main__":
    logfile = "/space/projects/live-viewer/logs/RingBuffer.log"
    targetDir = "/space/projects/live-viewer/data/zmq_target/"

    #enable logging
    helperScript.initLogging(logfile, True)

    ringbuffer = RingBuffer(2, targetDir)

    print "init"
    print ringbuffer

    # add a file
    newFileName1 = targetDir + "file1.tif"
    newFile = open(newFileName1, "w")
    newFile.write("test")
    newFile.close()

    newFileModTime = os.stat(newFileName1).st_mtime

    ringbuffer.add(newFileName1, newFileModTime)

    print "\nadded"
    print ringbuffer

    time.sleep(1)
    # add another file
    newFileName2 = targetDir + "file2.tif"
    newFile = open(newFileName2, "w")
    newFile.write("test")
    newFile.close()

    newFileModTime = os.stat(newFileName2).st_mtime

    ringbuffer.add(newFileName2, newFileModTime)

    print "\nadded"
    print ringbuffer

    try:
        os.remove(newFileName2)
    except:
        pass
    try:
        os.remove(newFileName1)
    except:
        pass

    print "== with content =="

    ringbuffer = RingBuffer(2, targetDir)

    print "init"
    print ringbuffer

    # add a file
    newFileName1 = targetDir + "file1.tif"
    newFile = open(newFileName1, "w")
    newFile.write("test")
    newFile.close()

    newFileModTime = os.stat(newFileName1).st_mtime

    ringbuffer.add(newFileName1, newFileModTime, "testContent")

    print "\nadded"
    print ringbuffer

    time.sleep(1)
    # add another file
    newFileName2 = targetDir + "file2.tif"
    newFile = open(newFileName2, "w")
    newFile.write("test")
    newFile.close()

    newFileModTime = os.stat(newFileName2).st_mtime

    ringbuffer.add(newFileName2, newFileModTime, "testContent")

    print "\nadded"
    print ringbuffer

    try:
        os.remove(newFileName1)
    except:
        pass
    try:
        os.remove(newFileName2)
    except:
        pass



