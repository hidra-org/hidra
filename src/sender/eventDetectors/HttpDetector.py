__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Jan Garrevoet <jan,garrevoet@desy.de>'

import os
import logging
import time
from logutils.queue import QueueHandler
import requests
import collections


class EventDetector():

    def __init__ (self, config, logQueue):

        self.log = self.getLogger(logQueue)

        # check format of config
        if ( not config.has_key("eigerIp") or
                not config.has_key("eigerApiVersion") or
                not config.has_key("historySize") ):
            self.log.error("Configuration of wrong format")
            self.log.debug("config={c}".format(c=config))
            checkPassed = False
        else:
            checkPassed = True
            self.log.info("Event detector configuration {c}".format(c=config))


        if checkPassed:
            self.session          = requests.session()

            self.eigerIp          = config["eigerIp"]
            self.eigerApiVersion  = config["eigerApiVersion"]
            self.eigerUrl         = "http://{ip}/filewriter/api/{api}/files".format(ip=self.eigerIp, api=self.eigerApiVersion)
            self.log.debug("Getting files from: {url}".format(url=self.eigerUrl))
#            http://192.168.138.37/filewriter/api/1.6.0/files

            # time to sleep after detector returned emtpy file list
            self.sleepTime        = 0.5

            # history to prevend double events
            self.files_downloaded = collections.deque(maxlen=config["historySize"])


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("HttpGetDetector")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def getNewEvent (self):

        eventMessageList = []

        files_stored = []

#        try:
            # returns a tuble of the form:
            # ('testp06/37_data_000001.h5', 'testp06/37_master.h5',
            #  'testp06/36_data_000003.h5', 'testp06/36_data_000002.h5',
            #  'testp06/36_data_000001.h5', 'testp06/36_master.h5')
#            files_stored = self.eigerdevice.read_attribute("FilesInBuffer", timeout=3).value
#        except Exception as e:
#            self.log.error("Getting 'FilesInBuffer'...failed." + str(e))
#            time.sleep(0.2)
#            return eventMessageList

        try:
            response = self.session.get(self.eigerUrl)
        except:
            self.log.error("Error in getting file list from {url}".format(url=self.eigerUrl), exc_info = True)

        try:
            response.raise_for_status()
#            self.log.debug("response: {r}".format(r=response.text))
            files_stored = response.json()
#            self.log.debug("files_stored: {f}".format(f=files_stored))
        except:
            self.log.error("Getting file list...failed.", exc_info=True)
            # Wait till next try to prevent denial of service
            time.sleep(self.sleepTime)
            return eventMessageList


        if not files_stored or set(files_stored).issubset(self.files_downloaded):
            # no new files received
            time.sleep(self.sleepTime)

        for file in files_stored:
            if file not in self.files_downloaded:
                ( relativePath, filename ) = os.path.split(file)
                eventMessage = {
                        "sourcePath"  : "http://{ip}/data".format(ip=self.eigerIp),
                        "relativePath": relativePath,
                        "filename"    : filename
                        }
                self.log.debug("eventMessage {m}".format(m=eventMessage))
                eventMessageList.append(eventMessage)
                self.files_downloaded.append(file)

        return eventMessageList


    def stop (self):
        pass


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


if __name__ == '__main__':
    import sys
    import time
    from subprocess import call
    from multiprocessing import Queue

    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))))
    SHARED_PATH  = BASE_PATH + os.sep + "src" + os.sep + "shared"
    print "SHARED", SHARED_PATH

    if not SHARED_PATH in sys.path:
        sys.path.append ( SHARED_PATH )
    del SHARED_PATH

    import helpers

    logfile  = BASE_PATH + os.sep + "logs" + os.sep + "zmqDetector.log"
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


#    detectorDevice   = "haspp10lab:10000/p10/eigerdectris/lab.01"
#    detectorDevice   = "haspp06:10000/p06/eigerdectris/exp.01"
#    filewriterDevice = "haspp10lab:10000/p10/eigerfilewriter/lab.01"
#    filewriterDevice = "haspp06:10000/p06/eigerfilewriter/exp.01"
#    eigerIp          = "192.168.138.52" #haspp11e1m
    eigerIp          = "131.169.55.170" #lsdma-lab04
    eigerApiVersion  = "1.5.0"
    config = {
            "eventDetectorType" : "HttpDetector",
            "eigerIp"           : eigerIp,
            "eigerApiVersion"   : eigerApiVersion,
            "historySize"       : 1000
            }



#    eventDetector = ZmqDetector(config, logQueue)
    eventDetector = EventDetector(config, logQueue)

    for i in range(5):
        try:
            eventList = eventDetector.getNewEvent()
            if eventList:
                print "eventList:", eventList

            time.sleep(1)
        except KeyboardInterrupt:
            break

    logQueue.put_nowait(None)
    logQueueListener.stop()
