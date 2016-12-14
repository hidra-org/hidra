//
//  Hello World server in C++
//  Binds REP socket to tcp://*:5555
//  Expects "Hello" from client, replies with "World"
//
#include <zmq.hpp>
#include <string>
#include <iostream>
#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>

#define sleep(n)    Sleep(n)
#endif


int main () {
    std::string signalHost = "*";
    std::string signalPort = "6000";


    //  Prepare our context and socket
    zmq::context_t context (1);
    zmq::socket_t socket (context, ZMQ_REP);

    std::string connectionStr("tcp://" + signalHost + ":" + signalPort);
    socket.bind (connectionStr.c_str());
//    socket.bind ("tcp://*:6000");

    int i = 0;
//    char m[7];
    std::string filename = "/opt/hidra/data/source/local/5.cbf";
    std::string m;

    while (true) {
        zmq::message_t request;

        //  Wait for next request from client
        socket.recv (&request);
        std::cout << "Received: " << static_cast<char*>(request.data()) << std::endl;

        //  Do some 'work'
        sleep(1);

        //  Send reply back to client
        m = "{ \"filePart\": " + std::to_string(i) + ", \"filename\": \"" + filename + "\" }";

//        sprintf(m, "world_%d", i);
//        std::cout << "sizeof(m) " << sizeof(m) << ", m " << m << std::endl;
        zmq::message_t reply (m.length());
        memcpy ( reply.data (), m.c_str(), m.length());
        socket.send (reply);

        i++;
    }
    return 0;
}
