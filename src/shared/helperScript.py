import os
import platform
import logging
import sys



def isWindows():
    returnValue = False
    windowsName = "Windows"
    platformName = platform.system()

    if platformName == windowsName:
        returnValue = True
    # osName = os.name
    # supportedWindowsNames = ["nt"]
    # if osName in supportedWindowsNames:
    #     returnValue = True

    return returnValue


def isLinux():
    returnValue = False
    linuxName = "Linux"
    platformName = platform.system()

    if platformName == linuxName:
        returnValue = True

    return returnValue



def isPosix():
    osName = os.name
    supportedPosixNames = ["posix"]
    returnValue = False

    if osName in supportedPosixNames:
        returnValue = True

    return returnValue



def isSupported():
    supportedWindowsReleases = ["7"]
    osRelease = platform.release()
    supportValue = False

    #check windows
    if isWindows():
        supportValue = True
        # if osRelease in supportedWindowsReleases:
        #     supportValue = True

    #check linux
    if isLinux():
        supportValue = True

    return supportValue


# This function is needed because configParser always needs a section name
# the used config file consists of key-value pairs only
# source: http://stackoverflow.com/questions/2819696/parsing-properties-file-in-python/2819788#2819788
class FakeSecHead(object):
    def __init__(self, fp):
        self.fp = fp
        self.sechead = '[asection]\n'

    def readline(self):
        if self.sechead:
            try:
                return self.sechead
            finally:
                self.sechead = None
        else:
            return self.fp.readline()



def checkDirExistance(dirPath):
    """
    abort if dir does not exist

    :return:
    """

    #check directory path for existance. exits if it does not exist
    if not os.path.exists(dirPath):
        logging.error("Dir '%s' does not exist. Abort." % str(dirPath))
        sys.exit(1)


def checkLogFileWritable(filepath, filename):
    #error if logfile cannot be written
    try:
        fullPath = os.path.join(filepath, filename)
        logFile = open(fullPath, "a")
    except:
        print "Unable to create the logfile """ + str(fullPath)
        print """Please specify a new target by setting the following arguments:
--logfileName
--logfilePath
"""
        sys.exit(1)


def initLogging(filenameFullPath, verbose):
    #@see https://docs.python.org/2/howto/logging-cookbook.html

#    def initLogging(self, logfilePath, verbose):
#
#        logfilePathFull = os.path.join(logfilePath, "cleaner.log")

    #more detailed logging if verbose-option has been set
    loggingLevel = logging.INFO
    if verbose:
        loggingLevel = logging.DEBUG

    #log everything to file
    logging.basicConfig(level=loggingLevel,
                        format='[%(asctime)s] [PID %(process)d] [%(filename)s] [%(module)s:%(funcName)s:%(lineno)d] [%(name)s] [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d_%H:%M:%S',
                        filename=filenameFullPath,
                        filemode="a")

#        fileHandler = logging.FileHandler(filename=logfilePathFull,
#                                          mode="a")
#        fileHandlerFormat = logging.Formatter(datefmt='%Y-%m-%d_%H:%M:%S',
#                                              fmt='[%(asctime)s] [PID %(process)d] [%(filename)s] [%(module)s:%(funcName)s] [%(name)s] [%(levelname)s] %(message)s')
#        fileHandler.setFormatter(fileHandlerFormat)
#        fileHandler.setLevel(loggingLevel)
#        logger.addHandler(fileHandler)

    #log info to stdout, display messages with different format than the file output
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s >  %(message)s")
    console.setFormatter(formatter)

    logging.getLogger("").addHandler(console)


