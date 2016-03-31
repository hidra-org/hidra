//
#include <zmq.hpp>
#include <string>
#include <iostream>
#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif


//class Receiver
int main() {

    std::string signalHost_ = "*";
    std::string signalPort_ = "6000";
    std::string extHost    = "0.0.0.0";
    std::string signalPort = "50050";
    std::string eventPort  = "50003";
    std::string dataPort   = "50100";


    //  Prepare our context and socket
    zmq::context_t context (1);
    zmq::socket_t socket (context, ZMQ_REP);

    std::string connectionStr("tcp://" + signalHost_ + ":" + signalPort_);
    socket.bind (connectionStr.c_str());


//        if context:
//            self.context    = context
//            self.extContext = True
//        else:
//            self.context    = zmq.Context()
//            self.extContext = False


    zmq::socket_t signalSocket  (context, ZMQ_REP);
    zmq::socket_t eventSocket (context, ZMQ_PULL);
    zmq::socket_t dataSocket (context, ZMQ_PULL);

//    std::string connectionStr;

    std::string connectionStr1("tcp://" + extHost + ":" + signalPort);
    signalSocket.bind(connectionStr1.c_str());
    std::cout << "signalSocket started (bind) for '" << connectionStr1 << "'";

    std::string  connectionStr2("tcp://" + extHost + ":" + eventPort);
    eventSocket.bind(connectionStr2.c_str());
    std::cout << "eventSocket started (bind) for '" << connectionStr2 << "'";

    std::string connectionStr3("tcp://" + extHost + ":" + dataPort);
    dataSocket.bind(connectionStr3.c_str());
    std::cout << "dataSocket started (bind) for '" << connectionStr3 << "'";

    zmq::message_t message;

    signalSocket.recv (&message);
    std::cout << "signalSocket recv: " << static_cast<char*>(message.data()) << std::endl;

    signalSocket.send (message);
    std::cout << "signalSocket send: " << static_cast<char*>(message.data()) << std::endl;

    zmq::message_t data;
    for (int i = 0; i < 5; i++)
    {
        eventSocket.recv (&data);
        std::cout << "eventSocket recv: " << static_cast<char*>(data.data()) << std::endl;
        dataSocket.recv (&data);
        std::cout << "dataSocket recv: " << static_cast<char*>(data.data()) << std::endl;
    }

    signalSocket.recv (&message);
    std::cout << "signalSocket recv: " << static_cast<char*>(message.data()) << std::endl;

    signalSocket.send (message);
    std::cout << "signalSocket send: " << static_cast<char*>(message.data()) << std::endl;

    eventSocket.recv (&data);
    std::cout << "eventSocket recv: " << static_cast<char*>(data.data()) << std::endl;

    return 0;
};


//
//receiverThread = Receiver(context)
//receiverThread.start()
//
//
//
//dataIngest obj;
//
//obj.createFile("1.h5")
//
//for (i=0; i < 5; i++):
//    std::string data = "asdfasdasdfasd"
//    obj.write(data)
//    std::cout << "write" << std::endl;
//
//obj.closeFile()
//
//logging.info("Stopping")
//
//receiverThread.stop()
