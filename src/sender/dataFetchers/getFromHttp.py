__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Jan Garrevoet <jan,garrevoet@desy.de>'

import zmq
import os
import sys
import logging
import cPickle
import urllib
import requests
import re
import urllib2
import time


def setup (log, prop):
    #TODO
    # check if prop has correct format

    prop["session"] = requests.session()


def getMetadata (log, metadata, chunkSize, localTarget = None):

    #extract fileEvent metadata
    try:
        #TODO validate metadata dict
        filename     = metadata["filename"]
        sourcePath   = metadata["sourcePath"]
        relativePath = metadata["relativePath"]
    except:
        log.error("Invalid fileEvent message received.", exc_info=True)
        log.debug("metadata=" + str(metadata))
        #skip all further instructions and continue with next iteration
        raise


    # no normpath used because that would transform http://... into http:/...
    sourceFilePath = sourcePath + os.sep + relativePath
    sourceFile     = os.path.join(sourceFilePath, filename)

    #TODO combine better with sourceFile... (for efficiency)
    if localTarget:
        targetFilePath = os.path.normpath(localTarget + os.sep + relativePath)
        targetFile     = os.path.join(targetFilePath, filename)
    else:
        targetFile = None

    try:
        log.debug("create metadata for source file...")
        #metadata = {
        #        "filename"     : filename,
        #        "sourcePath"   : sourcePath,
        #        "relativePath" : relativePath,
        #        "chunkSize"    : chunkSize
        #        }
        metadata[ "chunkSize" ] = chunkSize

        log.debug("metadata = " + str(metadata))
    except:
        log.error("Unable to assemble multi-part message.", exc_info=True)
        raise

    return sourceFile, targetFile, metadata


def dlProgress(count, blockSize, totalSize):
	percent = int(count*blockSize*100/totalSize)
	if percent > 100:
		percent = 100
	sys.stdout.write("\r %d%%" % percent)
	sys.stdout.flush()


def download1(file,dest):
	urllib.urlretrieve(file,dest,reporthook=dlProgress)


def delete_folders(url):
	print 'All data will be deleted from the Eiger.'
	print 'Data might be lost!!!!!!!!!!!!!!!!!!!!!!'
	print 'You have 10 s to press CTRL + c to cancel.'
	time.sleep(10)
	#Read folders
	urlcontent = urllib2.urlopen('http://192.168.138.37/data').read()
	folders = re.findall('<a href=.*</a>',urlcontent)
	for folder in folders:
		requests.delete(url+folder.split('"')[1])


def sendData (log, targets, sourceFile, metadata, openConnections, context, prop):

    response = prop["session"].get(sourceFile)
    if response.status_code != 200:
        raise

    try:
        chunkSize = metadata[ "chunkSize" ]
    except:
        log.error("Unable to get chunkSize", exc_info=True)

    chunkNumber = 0

    log.debug("Getting data for file '" + str(sourceFile) + "'...")
    #reading source file into memory
    for data in response.iter_content(chunk_size=chunkSize):
        log.debug("Packing multipart-message for file " + str(sourceFile) + "...")

        try:
            #assemble metadata for zmq-message
            metadataExtended = metadata.copy()
            metadataExtended["chunkNumber"] = chunkNumber
            metadataExtended = cPickle.dumps(metadataExtended)

            payload = []
            payload.append(metadataExtended)
            payload.append(data)
        except:
            log.error("Unable to pack multipart-message for file " + str(sourceFile), exc_info=True)

        #send message
        try:
            for target, prio in targets:

                # send data to the data stream to store it in the storage system
                if prio == 0:
                    # socket already known
                    if target in openConnections:
                        tracker = openConnections[target].send_multipart(payload, copy=False, track=True)
                        log.info("Sending message part from file " + str(sourceFile) +
                                 " to '" + target + "' with priority " + str(prio) )
                    else:
                        # open socket
                        socket        = context.socket(zmq.PUSH)
                        connectionStr = "tcp://" + str(target)

                        socket.connect(connectionStr)
                        log.info("Start socket (connect): '" + str(connectionStr) + "'")

                        # register socket
                        openConnections[target] = socket

                        # send data
                        tracker = openConnections[target].send_multipart(payload, copy=False, track=True)
                        log.info("Sending message part from file " + str(sourceFile) +
                                 " to '" + target + "' with priority " + str(prio) )

                    # socket not known
                    if not tracker.done:
                        log.info("Message part from file " + str(sourceFile) +
                                 " has not been sent yet, waiting...")
                        tracker.wait()
                        log.info("Message part from file " + str(sourceFile) +
                                 " has not been sent yet, waiting...done")

                else:
                    # socket already known
                    if target in openConnections:
                        # send data
                        openConnections[target].send_multipart(payload, zmq.NOBLOCK)
                        log.info("Sending message part from file " + str(sourceFile) +
                                 " to " + target)
                    # socket not known
                    else:
                        # open socket
                        socket        = context.socket(zmq.PUSH)
                        connectionStr = "tcp://" + str(target)

                        socket.connect(connectionStr)
                        log.info("Start socket (connect): '" + str(connectionStr) + "'")

                        # register socket
                        openConnections[target] = socket

                        # send data
                        openConnections[target].send_multipart(payload, zmq.NOBLOCK)
                        log.info("Sending message part from file " + str(sourceFile) +
                                 " to " + target)

            log.debug("Passing multipart-message for file " + str(sourceFile) + "...done.")

        except:
            log.error("Unable to send multipart-message for file " + str(sourceFile), exc_info=True)


def finishDataHandling (log, sourceFile, targetFile, prop):
    #TODO delete file from detector after sending
    responce = requests.delete(sourceFile)

    try:
        responce.raise_for_status()
        log.debug("Deleting file " + str(sourceFile) + " succeded.")
    except:
        log.error("Deleting file " + str(sourceFile) + " failed.", exc_info=True)


def clean (prop):
    pass


if __name__ == '__main__':
    from shutil import copyfile
    import subprocess

    try:
        BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))))
    except:
        BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) ))))
    print "BASE_PATH", BASE_PATH
    SHARED_PATH  = BASE_PATH + os.sep + "src" + os.sep + "shared"

    if not SHARED_PATH in sys.path:
        sys.path.append ( SHARED_PATH )
    del SHARED_PATH

    import helpers

    logfile = BASE_PATH + os.sep + "logs" + os.sep + "getFromHttp.log"
    logsize = 10485760

    # Get the log Configuration for the lisener
    h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose=True, onScreenLogLevel="debug")

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG) # Log level = DEBUG
    root.addHandler(h1)
    root.addHandler(h2)

    receivingPort    = "6005"
    receivingPort2   = "6006"
    extIp            = "0.0.0.0"
    dataFwPort       = "50010"

    context          = zmq.Context.instance()

    receivingSocket  = context.socket(zmq.PULL)
    connectionStr    = "tcp://{ip}:{port}".format( ip=extIp, port=receivingPort )
    receivingSocket.bind(connectionStr)
    logging.info("=== receivingSocket connected to " + connectionStr)

    receivingSocket2 = context.socket(zmq.PULL)
    connectionStr    = "tcp://{ip}:{port}".format( ip=extIp, port=receivingPort2 )
    receivingSocket2.bind(connectionStr)
    logging.info("=== receivingSocket2 connected to " + connectionStr)


    prework_sourceFile = BASE_PATH + os.sep + "test_file.cbf"

    #read file to send it in data pipe
    logging.debug("=== copy file to lsdma-lab04")
#    os.system('scp "%s" "%s:%s"' % (localfile, remotehost, remotefile) )
    subprocess.call("scp " + prework_sourceFile + " root@lsdma-lab04:/var/www/html/test_httpget/data", shell=True)

    workload = {
            "sourcePath"  : "http://131.169.55.170/test_httpget/data",
            "relativePath": "",
            "filename"    : "test_file.cbf"
            }
    targets = [['localhost:' + receivingPort, 1], ['localhost:' + receivingPort2, 0]]

    chunkSize       = 10485760 ; # = 1024*1024*10 = 10 MiB
    localTarget     = BASE_PATH + os.sep + "data" + os.sep + "target"
    openConnections = dict()

    dataFetcherProp = {
            "type"       : "getFromHttp",
            "session"    : None
            }

    setup(logging, dataFetcherProp)

    sourceFile, targetFile, metadata = getMetadata (logging, workload, chunkSize, localTarget = None)
#    sourceFile = "http://131.169.55.170/test_httpget/data/test_file.cbf"

    sendData(logging, targets, sourceFile, metadata, openConnections, context, dataFetcherProp)

    finishDataHandling(logging, sourceFile, targetFile, dataFetcherProp)

    logging.debug("openConnections after function call: " + str(openConnections))


    try:
        recv_message = receivingSocket.recv_multipart()
        logging.info("=== received: " + str(cPickle.loads(recv_message[0])))
        recv_message = receivingSocket2.recv_multipart()
        logging.info("=== received 2: " + str(cPickle.loads(recv_message[0])))
    except KeyboardInterrupt:
        pass
    finally:
        receivingSocket.close(0)
        receivingSocket2.close(0)
        clean(dataFetcherProp)
        context.destroy()

