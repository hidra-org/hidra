__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import os
import sys
import time
import zmq
import logging
import socket       # needed to get hostname

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) )
ZEROMQ_PATH = BASE_PATH + os.sep + "src" + os.sep + "ZeroMQTunnel"
CONFIG_PATH = BASE_PATH + os.sep + "conf"

print ZEROMQ_PATH

sys.path.append ( ZEROMQ_PATH )
sys.path.append ( CONFIG_PATH )

import helperScript


class ReceiverRealTimeAnalysis():
    senderComIp     = "127.0.0.1"
    senderComPort   = "6080"
    zmqContext      = None
    senderComSocket = None
    hostname        = socket.gethostname()

    def __init__(self, senderResponseTimeout = 1000):
        self.zmqContext = zmq.Context()
        assert isinstance(self.zmqContext, zmq.sugar.context.Context)

        self.socketResponseTimeout = senderResponseTimeout

        self.log = self.getLogger()
        self.log.debug("Init")

        self.senderComSocket = self.zmqContext.socket(zmq.REQ)
        # time to wait for the sender to give a confirmation of the signal
        self.senderComSocket.RCVTIMEO = self.socketResponseTimeout
        connectionStrSenderComSocket = "tcp://{ip}:{port}".format(ip=self.senderComIp, port=self.senderComPort)
        print "connectionStrSenderComSocket", connectionStrSenderComSocket
        self.senderComSocket.connect(connectionStrSenderComSocket)
        self.log.debug("senderComSocket started (connect) for '" + connectionStrSenderComSocket + "'")

        message = "START_REALTIME_ANALYSIS," + str(self.hostname)
        self.log.info("Sending start signal to sender...")
        self.log.debug("Sending start signal to sender, message: " + message)
        print "sending message ", message
        self.senderComSocket.send(str(message))
#        self.senderComSocket.send("START_LIVE_VIEWER")

        senderMessage = None
        try:
            senderMessage = self.senderComSocket.recv()
            print "answer to start live viewer: ", senderMessage
            self.log.debug("Received message from sender: " + str(senderMessage) )
        except KeyboardInterrupt:
            self.log.error("KeyboardInterrupt: No message received from sender")
            self.stop( sendToSender = False)
            sys.exit(1)
        except Exception as e:
            self.log.error("No message received from sender")
            self.log.debug("Error was: " + str(e))
            self.stop( sendToSender = False)
            sys.exit(1)

        if senderMessage == "START_REALTIME_ANALYSIS":
            self.log.info("Received confirmation from sender...start receiving files")
        else:
            print "Sending start signal to sender...failed."
            self.log.info("Sending start signal to sender...failed.")
            self.stop(sendToSender = False)

    def getLogger(self):
        logger = logging.getLogger("Receiver")
        return logger

    def askForNextFile(self):
        # get latest file from reveiver
        try:
            message = "NEXT_FILE,"+ str(self.hostname)
            print "Asking for next file"
            self.senderComSocket.send (message)
            #  Get the reply.
            received_file = self.senderComSocket.recv()
            print "Received_file", received_file
        except zmq.error.ZMQError:
            received_file = None
            print "ZMQError"

    def stop(self, sendToSender = True):

        if sendToSender:
            self.log.debug("sending stop signal to sender...")

            message = "STOP_REALTIME_ANALYSIS,"+ str(self.hostname)
            print "sending message ", message
            self.senderComSocket.send(str(message), zmq.NOBLOCK)

            try:
                senderMessage = self.senderComSocket.recv()
                print "answer to stop live viewer: ", senderMessage
                self.log.debug("Received message from sender: " + str(senderMessage) )

                if senderMessage == "STOP_REALTIME_ANALYSIS":
                    self.log.info("Received confirmation from sender...")
                else:
                    self.log.error("Received confirmation from sender...failed")
            except KeyboardInterrupt:
                self.log.error("KeyboardInterrupt: No message received from sender")
            except Exception as e:
                self.log.error("sending stop signal to sender...failed.")
                self.log.debug("Error was: " + str(e))

        # give the signal time to arrive
        time.sleep(0.1)

        # close ZeroMQ socket and destroy ZeroMQ context
        try:
            self.log.debug("closing signal communication sockets...")
            self.senderComSocket.close(linger=0)
            self.log.debug("closing signal communication sockets...done")
        except Exception as e:
            self.log.error("closing signal communication sockets...failed")
            self.log.debug("Error was: " + str(e))

        try:
            self.zmqContext.destroy()
            self.log.debug("closing zmqContext...done.")
        except Exception as e:
            self.log.debug("closing zmqContext...failed.")
            self.log.debug("Error was: " + str(e))



if __name__ == '__main__':
    logfilePath = "/home/kuhnm/Arbeit/live-viewer/logs/receiver_RealTimeAnalysis.log"
    verbose = True

    #enable logging
    helperScript.initLogging(logfilePath, verbose)

    receiver = ReceiverRealTimeAnalysis()

    i = 0
    while True:
        try:
            receiver.askForNextFile()
            time.sleep(1)
        except KeyboardInterrupt:
            receiver.stop()
            break
        if i >= 5:
            break
        else:
            i += 1


