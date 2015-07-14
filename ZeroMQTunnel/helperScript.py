import os
import platform



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
