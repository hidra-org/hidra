// API to ingest data into a data transfer unit

#ifndef DATAINGEST_H
#define DATAINGEST_H

#define version "0.0.1"

#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>


typedef int bool;
#define true 1
#define false 0

static char* s_recv (void *socket);

static int s_send (void * socket, char* string)


typedef struct {
    char *signalHost;
    char *extHost;

    char *signalPort;
    // has to be the same port as configured in dataManager.conf as eventPort
    char *eventPort;
    char *dataPort;

    //  Prepare our context and socket
    void *context;
    void *signalSocket;
    void *eventSocket;
    void *dataSocket;

    //  Initialize poll set
//    zmq::pollitem_t items [];

    char *filename;
    bool openFile;
    int  filePart;
    int  responseTimeout;
} dataIngest;


int dataIngest_init (dataIngest *dI);

int dataIngest_createFile (dataIngest *dI, char *fileName);

int dataIngest_write (dataIngest *dI, char *data, int size);

int dataIngest_closeFile (dataIngest *dI);

int dataIngest_stop (dataIngest *dI);

#endif

