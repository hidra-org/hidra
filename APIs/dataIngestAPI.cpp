# API to ingest data into a data transfer unit

DataIngest::DataIngest ()
{

    std::string connectionStr("tcp://" + signalHost + ":" + signalPort);
    signalSocket.connect(connectionStr.c_str());
    std::cout << "signalSocket started (connect) for '" << connectionStr << "'";
//            self.log.error("Failed to start signalSocket (connect): '" + connectionStr + "'", exc_info=True)
//            subscriber.setsockopt(ZMQ_SUBSCRIBE, "10001 ", 6);
//#        self.signalSocket.RCVTIMEO = self.responseTimeout

    //  Initialize poll set
    zmq::pollitem_t items [] = {
        { signalSocket, 0, ZMQ_POLLIN, 0 },
    };

    std::string connectionStr("tcp://localhost:" + eventPort);
    eventSocket.connect(connectionStr.c_str());
    std::cout << "eventSocket started (connect) for '" << connectionStr << "'";
//            self.log.error("Failed to start eventSocket (connect): '" + connectionStr + "'", exc_info=True)

    std::string connectionStr("tcp://localhost:" + dataPort);
    dataSocket.connect(connectionStr.c_str());
    std::cout << "dataSocket started (connect) for '" << connectionStr << "'";
//            self.log.error("Failed to start dataSocket (connect): '" + connectionStr + "'", exc_info=True)


};


int DataIngest::createFile (std::string fileName)
{

//        if self.openFile and self.openFile != filename:
//            raise Exception("File " + str(filename) + " already opened.")

    // Send notification to receiver
    signalSocket.send("OPEN_FILE")
    std::cout << "Sending signal to open a new file.";

    zmq::message_t response;
    signalSocket.recv(&response)
    std::cout << "Received responce:" << static_cast<char*>(response.data()) << std::endl;

    filename = fileName
    filePart = 0

};

void my_free (void *data, void *hint)
{
    free (data);
};


int DataIngest::write (void *data, int &size)
{

    std::string message = "{ \"filePart\": " + std::to_string(filePart) + ", \"filename\": \"" + filename + "\" }";


    zmq::message_t eventMessage (message.length());
    memcpy ( eventMessage.data (), message.c_str(), message.length());

    // Send event to eventDetector
    eventSocket.send(eventMessage);

    zmq::message_t dataMessage (data, size, my_free);

//    zmq::message_t dataMessage (size);
//    memcpy ( dataMessage.data (), &data, size);

    // Send data to ZMQ-Queue
    dataSocket.send(dataMessage);

    filePart += 1

};

int DataIngest::closeFile()
{

    zmq::message_t resvMessage;
    char* sendMessage = "CLOSE_FILE";

    // Send close-signal to signal socket
    signalSocket.send(sendMessage);
    std::cout << "Sending signal to close the file to signalSocket.";
    //        self.log.error("Sending signal to close the file to signalSocket...failed.")


    // send close-signal to event Detector
    eventSocket.send(sendMessage)
    std::cout << "Sending signal to close the file to eventSocket.(sendMessage=" << sendMessage << ")";
//        self.log.error("Sending signal to close the file to eventSocket...failed.)")


//    try:
//        socks = dict(self.poller.poll(10000)) # in ms
//    except:
//        socks = None
//        self.log.error("Could not poll for signal", exc_info=True)
//
//    if socks and self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:
    std::cout << "Received answer to signal..." << std:endl;
    //  Get the reply.
    signalSocket.recv(&resvMessage)
    std::cout << "Received answer to signal: " + static_cast<char*>(recvMessage) << std::endl;
//    else:
//        recvMessage = None


    if ( recvMessage != sendMessage )
    {
        std::cout << "recieved message: " << recvMessage << ")" << std::endl;
        std::cout << "send message: " + sendMessage << std::endl;
//            raise Exception("Something went wrong while notifying to close the file")

    };

    openFile = ""
    filePart = 0

};

DataIngest::~DataIngest()
{

//        try:
//            if self.signalSocket:
//                self.log.info("closing eventSocket...")
//                self.signalSocket.close(linger=0)
//                self.signalSocket = None
//            if self.eventSocket:
//                self.log.info("closing eventSocket...")
//                self.eventSocket.close(linger=0)
//                self.eventSocket = None
//            if self.dataSocket:
//                self.log.info("closing dataSocket...")
//                self.dataSocket.close(linger=0)
//                self.dataSocket = None
//        except:
//            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)
//
//        # if the context was created inside this class,
//        # it has to be destroyed also within the class
//        if not self.extContext and self.context:
//            try:
//                self.log.info("Closing ZMQ context...")
//                self.context.destroy()
//                self.context = None
//                self.log.info("Closing ZMQ context...done.")
//            except:
//                self.log.error("Closing ZMQ context...failed.", exc_info=True)

};

