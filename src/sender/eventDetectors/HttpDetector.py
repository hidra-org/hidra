__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Jan Garrevoet <jan,garrevoet@desy.de>'


import os
import logging
import time
from logutils.queue import QueueHandler
import collections
try:
    import PyTango
except:
    import sys
    sys.path.insert(0, "/usr/local/lib64/python2.6/site-packages")
    import PyTango


class EventDetector():

    def __init__ (self, config, logQueue):

        self.log = self.getLogger(logQueue)

        # check format of config
        checkPassed = True
        if ( not config.has_key("prefix") or
                not config.has_key("detectorDevice") or
                not config.has_key("filewriterDevice") or
                not config.has_key("historySize") ):
            self.log.error ("Configuration of wrong format")
            self.log.debug ("config="+ str(config))
            checkPassed = False

        if checkPassed:
            self.detectorDevice_conf   = config["detectorDevice"]
            self.fileWriterDevice_conf = config["filewriterDevice"]

            try:
                self.eigerdevice      = PyTango.DeviceProxy (self.detectorDevice_conf)
                self.log.info("Starting the detector device server '" + self.detectorDevice_conf + "'.")
            except:
                self.log.error("Starting the detector device server '" + self.detectorDevice_conf + "'...failed.", exc_info=True)

            try:
                self.filewriterdevice = PyTango.DeviceProxy (self.fileWriterDevice_conf)
                self.log.info("Starting the filewriter device server '" + self.fileWriterDevice_conf + "'.")
            except:
                self.log.error("Starting the filewriter device server '" + self.fileWriterDevice_conf + "'...failed.", exc_info=True)

            if config["prefix"] == "":
                self.current_dataset_prefix = ""
            else:
                self.current_dataset_prefix = config["prefix"]

            try:
                self.EigerIP          = self.eigerdevice.get_property('Host').get('Host')[0]
            except:
                self.log.error("Getting EigerIP...failed.", exc_info=True)

            try:
                self.images_per_file  = self.filewriterdevice.read_attribute("ImagesPerFile").value
                self.FrameTime        = self.eigerdevice.read_attribute("FrameTime").value
            except:
                self.log.error("Getting attributes...failed.", exc_info=True)
                self.images_per_file = 1
                self.FrameTime       = 0.5


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

        try:
            # returns a tuble of the form:
            # ('testp06/37_data_000001.h5', 'testp06/37_master.h5', 'testp06/36_data_000007.h5', 'testp06/36_data_000006.h5', 'testp06/36_data_000005.h5', 'testp06/36_data_000004.h5', 'testp06/36_data_000003.h5', 'testp06/36_data_000002.h5', 'testp06/36_data_000001.h5', 'testp06/36_master.h5')
            files_stored = self.eigerdevice.read_attribute("FilesInBuffer", timeout=3).value

        except PyTango.CommunicationFailed:
            self.log.info("Getting 'FilesInBuffer'...failed due to PyTango.CommunicationFailed.", exc_info=True)

            # I don't think I need this
            try:
                self.eigerdevice      = PyTango.DeviceProxy (self.detectorDevice_conf)
                self.log.info("Starting the detector device server '" + self.detectorDevice_conf + "'.")
            except:
                self.log.error("Starting the detector device server '" + self.detectorDevice_conf + "'...failed.", exc_info=True)

            try:
                self.filewriterdevice = PyTango.DeviceProxy (self.fileWriterDevice_conf)
                self.log.info("Starting the filewriter device server '" + self.fileWriterDevice_conf + "'.")
            except:
                self.log.error("Starting the filewriter device server '" + self.fileWriterDevice_conf + "'...failed.", exc_info=True)
            time.sleep(0.2)
            return eventMessageList
        except:
            self.log.error("Getting 'FilesInBuffer'...failed.", exc_info=True)
            time.sleep(0.2)
            return eventMessageList

        if not files_stored or set(files_stored).issubset(self.files_downloaded):
            time.sleep(self.images_per_file * self.FrameTime)

        ## ===== Look for current measurement files
        if files_stored:
            available_files = [file for file in files_stored if file.startswith(self.current_dataset_prefix)]
        else:
            available_files = []


        #TODO needed format: list of dictionaries of the form
        # {
        #     "filename"     : filename,
        #     "sourcePath"   : sourcePath,
        #     "relativePath" : relativePath
        # }

        for file in available_files:
            if file not in self.files_downloaded:
                ( relativePath, filename ) = os.path.split(file)
                eventMessage = {
                        "sourcePath"  : "http://" + self.EigerIP + "/data",
                        "relativePath": relativePath,
                        "filename"    : filename
                        }
                self.log.debug("eventMessage" + str(eventMessage))
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
    detectorDevice   = "haspp06:10000/p06/eigerdectris/exp.01"
#    filewriterDevice = "haspp10lab:10000/p10/eigerfilewriter/lab.01"
    filewriterDevice = "haspp06:10000/p06/eigerfilewriter/exp.01"
    config = {
            "eventDetectorType" : "httpget",
            "prefix"            : "",
            "detectorDevice"    : detectorDevice,
            "filewriterDevice"  : filewriterDevice,
            "historySize"       : 1000
            }



#    eventDetector = ZmqDetector(config, logQueue)
    eventDetector = EventDetector(config, logQueue)

    sourceFile = BASE_PATH + os.sep + "test_file.cbf"
    targetFileBase = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep


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
