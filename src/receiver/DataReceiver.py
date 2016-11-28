__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import sys
import argparse
import logging
import os
import json
import subprocess
import re
try:
    import ConfigParser
except:
    import configparser as ConfigParser


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"
API_PATH    = BASE_PATH + os.sep + "APIs"
CONFIG_PATH = BASE_PATH + os.sep + "conf"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH
del BASE_PATH

import helpers

try:
    # search in global python modules first
    from hidra.transfer import dataTransfer
except:
    # then search in local modules
    if not API_PATH in sys.path:
        sys.path.append ( API_PATH )
    del API_PATH

    from hidra.transfer import dataTransfer


def argumentParsing():
    defaultConfig = CONFIG_PATH + os.sep + "dataReceiver.conf"

    ##################################
    #   Get command line arguments   #
    ##################################

    parser = argparse.ArgumentParser()

    parser.add_argument("--configFile"        , type    = str,
                                                help    = "Location of the configuration file")

    parser.add_argument("--logfilePath"       , type    = str,
                                                help    = "Path where logfile will be created")
    parser.add_argument("--logfileName"       , type    = str,
                                                help    = "Filename used for logging")
    parser.add_argument("--logfileSize"       , type    = int,
                                                help    = "File size before rollover in B (linux only)")
    parser.add_argument("--verbose"           , help    = "More verbose output",
                                                action  = "store_true" )
    parser.add_argument("--onScreen"          , type    = str,
                                                help    = "Display logging on screen (options are CRITICAL, ERROR, WARNING, INFO, DEBUG)",
                                                default = False )

    parser.add_argument("--whitelist"         , type    = str,
                                                help    = "List of hosts allowed to connect")
    parser.add_argument("--targetDir"         , type    = str,
                                                help    = "Where incoming data will be stored to")
    parser.add_argument("--dataStreamIp"      , type    = str,
                                                help    = "Ip of dataStream-socket to pull new files from")
    parser.add_argument("--dataStreamPort"    , type    = str,
                                                help    = "Port number of dataStream-socket to pull new files from")

    arguments   = parser.parse_args()
    arguments.configFile         = arguments.configFile or defaultConfig

    # check if configFile exist
    helpers.checkFileExistance(arguments.configFile)

    ##################################
    # Get arguments from config file #
    ##################################

    config = ConfigParser.RawConfigParser()
    config.readfp(helpers.FakeSecHead(open(arguments.configFile)))

    arguments.logfilePath        = arguments.logfilePath        or config.get('asection', 'logfilePath')
    arguments.logfileName        = arguments.logfileName        or config.get('asection', 'logfileName')

    if not helpers.isWindows():
        arguments.logfileSize    = arguments.logfileSize        or config.get('asection', 'logfileSize')

    try:
        arguments.whitelist      = arguments.whitelist          or json.loads(config.get('asection', 'whitelist'))
    except ValueError:
        ldap_cn = config.get('asection', 'whitelist')
        p = subprocess.Popen(["ldapsearch",  "-x", "-H ldap://it-ldap-slave.desy.de:1389", "cn=" + ldap_cn , "-LLL"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        lines = p.stdout.readlines()

        matchHost = re.compile(r'nisNetgroupTriple: [(]([\w|\S|.]+),.*,[)]', re.M|re.I)
        arguments.whitelist = []

        for line in lines:

            if matchHost.match(line):
                if matchHost.match(line).group(1) not in arguments.whitelist:
                    arguments.whitelist.append(matchHost.match(line).group(1))
    except:
        arguments.whitelist      = json.loads(config.get('asection', 'whitelist').replace("'", '"'))

    arguments.targetDir          = arguments.targetDir          or config.get('asection', 'targetDir')

    arguments.dataStreamIp       = arguments.dataStreamIp       or config.get('asection', 'dataStreamIp')
    arguments.dataStreamPort     = arguments.dataStreamPort     or config.get('asection', 'dataStreamPort')

    ##################################
    #     Check given arguments      #
    ##################################

    logfile     = os.path.join(arguments.logfilePath, arguments.logfileName)

    #enable logging
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handlers = helpers.getLogHandlers(logfile, arguments.logfileSize, arguments.verbose, arguments.onScreen)

    if type(handlers) == tuple:
        for h in handlers:
            root.addHandler(h)
    else:
        root.addHandler(handlers)

    # check target directory for existance
    helpers.checkDirExistance(arguments.targetDir)

    # check if logfile is writable
    helpers.checkLogFileWritable(arguments.logfilePath, arguments.logfileName)

    return arguments


class DataReceiver:
    def __init__(self):
        self.dataTransfer = None

        try:
            arguments = argumentParsing()
        except:
            self.log = self.getLogger()
            raise

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
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping due to unknown error condition", exc_info=True)
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
                self.dataTransfer.store(self.targetDir)
            except KeyboardInterrupt:
                break
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
