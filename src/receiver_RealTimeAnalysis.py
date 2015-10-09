__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import os
import sys
import time
import zmq
import logging
import socket       # needed to get hostname

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) )
#ZEROMQ_PATH = BASE_PATH + os.sep + "src" + os.sep + "ZeroMQTunnel"
CONFIG_PATH = BASE_PATH + os.sep + "conf"


#sys.path.append ( ZEROMQ_PATH )
sys.path.append ( CONFIG_PATH )

import helperScript


class ReceiverRealTimeAnalysis():
    senderComIp     = "127.0.0.1"
    senderComPort   = "6080"
    senderDataIp    = "127.0.0.1"
    senderDataPort  = "6081"
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
        connectionStr = "tcp://{ip}:{port}".format(ip=self.senderComIp, port=self.senderComPort)
        self.senderComSocket.connect(connectionStr)
        self.log.debug("senderComSocket started (connect) for '" + connectionStr + "'")
        print "senderComSocket started (connect) for '" + connectionStr + "'"

        self.senderDataSocket = self.zmqContext.socket(zmq.REQ)
        # time to wait for the sender to give a confirmation of the signal
        connectionStr = "tcp://{ip}:{port}".format(ip=self.senderDataIp, port=self.senderDataPort)
        self.senderDataSocket.connect(connectionStr)
        self.log.debug("senderDataSocket started (connect) for '" + connectionStr + "'")
        print "senderDataSocket started (connect) for '" + connectionStr + "'"

        message = "START_REALTIME_ANALYSIS," + str(self.hostname)
        self.log.info("Sending start signal to sender...")
        self.log.debug("Sending start signal to sender, message: " + message)
        print "sending message ", message
        self.senderComSocket.send(str(message))
#        self.senderComSocket.send("START_LIVE_VIEWER")

        senderMessage = None
        try:
            senderMessage = self.senderComSocket.recv()
            print "answer to start realtime analysis: ", senderMessage
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
        message = "NEXT_FILE"
        while True:
            try:
                print "Asking for next file"
                self.log.debug("Asking for next file")
                self.senderDataSocket.send(message)
                self.log.debug("Asking for next file...done")
                print "Asking for next file...done"

                time.sleep(1)

                try:
                    #  Get the reply.
                    received_file = self.senderDataSocket.recv_multipart()
#                    print "Received_file", "".join(received_file)[:45]
                    print "Received_file", received_file[0][:45]
                    self.log.debug("Received_file" + str(received_file))
                except zmq.error.ZMQError:
                    received_file = None
                    self.log.warning("Unable to reveice reply: sender is currently busy")
                except Exception as e:
                    self.log.error("Unable receive reply")
                    self.log.debug("Error was: " + str(e))
                break

            except Exception as e:
                self.log.error("Unable to send request")
                self.log.debug("Error was: " + str(e))


    def stop(self, sendToSender = True):

        if sendToSender:
            self.log.debug("sending stop signal to sender...")

            message = "STOP_REALTIME_ANALYSIS,"+ str(self.hostname)
            print "sending message ", message
            try:
                self.senderComSocket.send(str(message), zmq.NOBLOCK)
            except zmq.error.ZMQError:
                self.log.debug("Unable to send stop signal to sender")

            try:
                senderMessage = self.senderComSocket.recv()
                print "answer to stop realtime analysis: ", senderMessage
                self.log.debug("Received message from sender: " + str(senderMessage) )

                if senderMessage == "STOP_REALTIME_ANALYSIS":
                    self.log.info("Received confirmation from sender...")
                else:
                    self.log.debug("Received unexpected response from sender")
                    self.log.debug("Try to send stop signal again")

                    try:
                        self.senderComSocket.send(str(message), zmq.NOBLOCK)
                    except zmq.error.ZMQError:
                        self.log.debug("Unable to send stop signal to sender (second try)")

                    senderMessage = self.senderComSocket.recv()
                    print "answer to stop realtime analysis(second try): ", senderMessage
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
            self.log.debug("closing sockets...")
            self.senderComSocket.close(linger=0)
            self.senderDataSocket.close(linger=0)
            self.log.debug("closing sockets...done")
        except Exception as e:
            self.log.error("closing sockets...failed")
            self.log.debug("Error was: " + str(e))

        try:
            self.zmqContext.destroy()
            self.log.debug("closing zmqContext...done.")
        except Exception as e:
            self.log.debug("closing zmqContext...failed.")
            self.log.debug("Error was: " + str(e))



if __name__ == '__main__':
    logfilePath = BASE_PATH + os.sep + "logs/receiver_RealTimeAnalysis.log"
    verbose = True

    #enable logging
    helperScript.initLogging(logfilePath, verbose)

    receiver = ReceiverRealTimeAnalysis()

    time.sleep(0.5)
    i = 0
    while True:
        try:
            receiver.askForNextFile()
            time.sleep(1)
        except KeyboardInterrupt:
            break
#        if i >= 5:
#            break
#        else:
#            i += 1

    receiver.stop()


