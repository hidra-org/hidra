__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import sys
import argparse
import logging
import os
import ConfigParser
import json
import subprocess
import re


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"
API_PATH    = BASE_PATH + os.sep + "APIs"
CONFIG_PATH = BASE_PATH + os.sep + "conf"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH
del BASE_PATH

from dataTransferAPI import dataTransfer


def argumentParsing():
    configFile = CONFIG_PATH + os.sep + "dataReceiver.conf"

    config = ConfigParser.RawConfigParser()
    config.readfp(helpers.FakeSecHead(open(configFile)))

    logfilePath    = config.get('asection', 'logfilePath')
    logfileName    = config.get('asection', 'logfileName')
    logfileSize    = config.get('asection', 'logfileSize')

    try:
        whitelist      = json.loads(config.get('asection', 'whitelist'))
    except ValueError:
        ldap_cn = config.get('asection', 'whitelist')
        p = subprocess.Popen("ldapsearch -x -H ldap://it-ldap-slave.desy.de:1389 cn=" + ldap_cn + " -LLL", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        lines = p.stdout.readlines()

        matchHost = re.compile(r'nisNetgroupTriple: [(]([\w|\S|.]+),.*,[)]', re.M|re.I)
        whitelist = []

        for line in lines:

            if matchHost.match(line):
                if matchHost.match(line).group(1) not in whitelist:
                    whitelist.append(matchHost.match(line).group(1))


    targetDir      = config.get('asection', 'targetDir')

    dataStreamIp   = config.get('asection', 'dataStreamIp')
    dataStreamPort = config.get('asection', 'dataStreamPort')


    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"       , type    = str,
                                                help    = "Path where logfile will be created (default=" + str(logfilePath) + ")",
                                                default = logfilePath )
    parser.add_argument("--logfileName"       , type    = str,
                                                help    = "Filename used for logging (default=" + str(logfileName) + ")",
                                                default = logfileName )
    parser.add_argument("--logfileSize"       , type    = int,
                                                help    = "File size in B before rollover (linux only; (default=" + str(logfileSize) + ")",
                                                default = logfileSize )
    parser.add_argument("--verbose"           , help    = "More verbose output",
                                                action  = "store_true" )
    parser.add_argument("--onScreen"          , type    = str,
                                                help    = "Display logging on screen (options are CRITICAL, ERROR, WARNING, INFO, DEBUG)",
                                                default = False )

    parser.add_argument("--whitelist"         , type    = str,
                                                help    = "List of hosts allowed to connect (default=" + str(whitelist) + ")",
                                                default = whitelist )
    parser.add_argument("--targetDir"         , type    = str,
                                                help    = "Where incoming data will be stored to (default=" + str(targetDir) + ")",
                                                default = targetDir )
    parser.add_argument("--dataStreamIp"      , type    = str,
                                                help    = "Ip of dataStream-socket to pull new files from (default=" + str(dataStreamIp) + ")",
                                                default = dataStreamIp )
    parser.add_argument("--dataStreamPort"    , type    = str,
                                                help    = "Port number of dataStream-socket to pull new files from (default=" + str(dataStreamPort) + ")",
                                                default = dataStreamPort )


    arguments   = parser.parse_args()

    logfilePath = arguments.logfilePath
    logfileName = arguments.logfileName
    logfile     = os.path.join(logfilePath, logfileName)
    logsize     = arguments.logfileSize
    verbose     = arguments.verbose
    onScreen    = arguments.onScreen

    targetDir   = arguments.targetDir

    #enable logging
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handlers = helpers.getLogHandlers(logfile, logsize, verbose, onScreen)

    if type(handlers) == tuple:
        for h in handlers:
            root.addHandler(h)
    else:
        root.addHandler(handlers)

    # check target directory for existance
    helpers.checkDirExistance(targetDir)

    # check if logfile is writable
    helpers.checkLogFileWritable(logfilePath, logfileName)

    return arguments


class DataReceiver:
    def __init__(self):
        arguments = argumentParsing()

        self.log          = self.getLogger()

        self.whitelist    = arguments.whitelist

        self.log.info("Configured whitelist: " + str(self.whitelist))

        self.targetDir    = os.path.normpath(arguments.targetDir)
        self.dataIp       = arguments.dataStreamIp
        self.dataPort     = arguments.dataStreamPort

        self.log.info("Writing to directory '" + self.targetDir + "'.")

        self.dataTransfer = dataTransfer("stream", useLog = True)

        try:
            self.run()
        finally:
            self.stop()


    def getLogger(self):
        logger = logging.getLogger("DataReceiver")
        return logger


    def run(self):

        try:
            self.dataTransfer.start([self.dataIp, self.dataPort], self.whitelist)
#            self.dataTransfer.start(self.dataPort)
        except:
            self.log.error("Could not initiate stream", exc_info=True)
            raise



        continueReceiving = True #receiving will stop if value gets False
        self.log.debug("Waiting for new messages...")
        #run loop, and wait for incoming messages
        while continueReceiving:
            try:
                [payloadMetadata, payload] = self.dataTransfer.get()
            except:
                break

            try:
                self.dataTransfer.store(self.targetDir, [payloadMetadata, payload] )
            except:
                self.log.error("Storing data...failed.", exc_info=True)
                raise


    def stop(self):
        if self.dataTransfer:
            self.log.info("Shutting down receiver...")
            self.dataTransfer.stop()
            self.dataTransfer = None


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()


if __name__ == "__main__":
    #start file receiver
    receiver = DataReceiver()
