# API to ingest data into a data transfer unit

#ifndef DATAINGEST_H
#define DATAINGEST_H

#define version "0.0.1"

#include <zmq.hpp>
#include <string>

//import logging
//import cPickle
//import traceback

//typedef std::map<std::string, int> Dict;


class DataIngest
{
    std::string     signalHost = "zitpcx19282";

    std::string     signalPort = "50050";
    // has to be the same port as configured in dataManager.conf as eventPort
    std::string     eventPort  = "50003";
    std::string     dataPort   = "50010";

    zmq::context_t  context (1);

    zmq::socket_t   signalSocket (context, ZMQ_REQ);
    zmq::socket_t   eventSocket (context, ZMQ_PUSH);
    zmq::socket_t   dataSocket (context, ZMQ_PUSH);

    //  Initialize poll set
    zmq::pollitem_t items [];

    std::string     self.filename;
    bool            openFile;
    int             filePart;
    int             responseTimeout    = 1000;


    DataIngest ();

    createFile (std::string filename);
    write(data);
    closeFile();
    stop();

    ~DataIngest();

};

#endif

