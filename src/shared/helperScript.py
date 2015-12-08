import os
import platform
import logging
import sys
import shutil
import zmq


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

# http://code.activestate.com/recipes/541096-prompt-the-user-for-confirmation/
def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.

    'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.

    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n:
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y:
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True

    """

    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        try:
            ans = raw_input(prompt)
        except KeyboardInterrupt:
            logging.error("Keyboard Interruption detected.")
            sys.exit(1)
        except Exception as e:
            logging.error("Something went wrong with the confirmation.")
            logging.debug("Error was: " + str(e))
            break

        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            logging.error("please enter y or n.")
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False


def checkDirEmpty(dirPath):

    #check if directory is empty
    if os.listdir(dirPath):
        logging.debug("Directory '%s' is not empty." % str(dirPath))
        if confirm(prompt="Directory " + str(dirPath) + " is not empty.\nShould its content be removed?",
                   resp = True):
            for element in os.listdir(dirPath):
                path = dirPath + os.sep + element
                if os.path.isdir(path):
                   try:
                        os.rmdir(path)
                   except OSError:
                        shutil.rmtree(path)
                else:
                    os.remove(path)
            logging.info("All elements of directory " + str(dirPath) + " were removed.")




def checkSubDirExistance(dirPath, subDirs):
    """
    abort if dir does not exist

    :return:
    """
    dirPath = os.path.normpath(dirPath)
    dirsToCheck = [dirPath + os.sep + directory for directory in subDirs]
    noSubdir = True

    for d in dirsToCheck:
        #check directory path for existance. exits if it does not exist
        if os.path.exists(d):
            noSubdir = False

    if noSubdir:
        logging.error("There are none of the specified subdirectories inside '%s'. Abort." % str(dirPath))
        logging.error("Checked paths: " + str(dirsToCheck))
        sys.exit(1)


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


def extractSignal(message, log):
    try:
        messageSplit = message.split(',')
    except Exception as e:
        log.info("Received signal is of the wrong format")
        log.debug("Received signal: " + str(message))
        return None, None, None

    if len(messageSplit) < 3:
        log.info("Received signal is of the wrong format")
        log.debug("Received signal is too long: " + str(message))
        return None, None, None

    signal   = messageSplit[0]
    hostname = messageSplit[1]
    port     = messageSplit[2]

    return signal, hostname, port


def checkSignal(hostname, whiteList, socket, log):

    if hostname.endswith(".desy.de"):
        hostnameModified = hostname[:-8]
    else:
        hostnameModified = hostname

    if hostname in whiteList or hostnameModified in whiteList:
        return True
    else:
        return False




def initLogging(filenameFullPath, verbose, onScreenLogLevel = False):
    #@see https://docs.python.org/2/howto/logging-cookbook.html

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

#        fileHandler = logging.FileHandler(filename=filenameFullPath,
#                                          mode="a")
#        fileHandlerFormat = logging.Formatter(datefmt='%Y-%m-%d_%H:%M:%S',
#                                              fmt='[%(asctime)s] [PID %(process)d] [%(filename)s] [%(module)s:%(funcName)s] [%(name)s] [%(levelname)s] %(message)s')
#        fileHandler.setFormatter(fileHandlerFormat)
#        fileHandler.setLevel(loggingLevel)
#        logging.getLogger("").addHandler(fileHandler)

    #log info to stdout, display messages with different format than the file output
    if onScreenLogLevel:
        if onScreenLogLevel in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:

            console = logging.StreamHandler()
            screenHandlerFormat = logging.Formatter(datefmt = "%Y-%m-%d_%H:%M:%S",
                                                    fmt     = "[%(asctime)s] > %(message)s")

            if onScreenLogLevel == "DEBUG":
                screenLoggingLevel = logging.DEBUG
                console.setLevel(screenLoggingLevel)

                screenHandlerFormat = logging.Formatter(datefmt = "%Y-%m-%d_%H:%M:%S",
                                                        fmt     = "[%(asctime)s] > [%(filename)s:%(lineno)d] %(message)s")

                if not verbose:
                    logging.error("Logging on Screen: Option DEBUG in only active when using verbose option as well (Fallback to INFO).")
            elif onScreenLogLevel == "INFO":
                screenLoggingLevel = logging.INFO
                console.setLevel(screenLoggingLevel)
            elif onScreenLogLevel == "WARNING":
                screenLoggingLevel = logging.WARNING
                console.setLevel(screenLoggingLevel)
            elif onScreenLogLevel == "ERROR":
                screenLoggingLevel = logging.ERROR
                console.setLevel(screenLoggingLevel)
            elif onScreenLogLevel == "CRITICAL":
                screenLoggingLevel = logging.CRITICAL
                console.setLevel(screenLoggingLevel)

            console.setFormatter(screenHandlerFormat)
            logging.getLogger("").addHandler(console)
        else:
            logging.error("Logging on Screen: Option " + str(onScreenLogLevel) + " is not supported.")


