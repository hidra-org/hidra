// API to ingest data into a data transfer unit

//#ifndef DATAINGEST_H
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

// helper functions for sending and receiving messages
// source: Pieter Hintjens: ZeroMQ; O'Reilly
static char* s_recv (void *socket)
{
    zmq_msg_t message;
    zmq_msg_init (&message);
    int size = zmq_msg_recv (&message, socket, 0);
    if (size == -1)
        return NULL;
    char *string = malloc (size + 1);
    memcpy (string, zmq_msg_data (&message), size);
    zmq_msg_close (&message);
    string [size] = 0;
    return string;
};

static int s_send (void * socket, char* string)
{
    int size = zmq_send (socket, string, strlen (string), 0);
    return size;

};


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


int dataIngest_init (dataIngest *dI)
{
    dI->signalHost = "zitpcx19282";
    dI->extHost    = "0.0.0.0";

    dI->signalPort = "50050";
    dI->eventPort  = "50003";
    dI->dataPort   = "50010";
//
//	if ( (dI->context = zmq_ctx_new ()) == NULL )
//    {
//		perror("ERROR: Cannot create 0MQ context: ");
//		exit(9);
//	}
//
//	if ( (dI->signalSocket = zmq_socket (dI->context, ZMQ_REQ)) == NULL )
//    {
//		perror("ERROR: Could not create 0MQ socket: ");
//		exit(9);
//	}
//
//	if ( (dI->eventSocket = zmq_socket (dI->context, ZMQ_PUSH)) == NULL )
//    {
//		perror("ERROR: Could not create 0MQ socket: ");
//		exit(9);
//	}
//
//    if ( (dI->dataSocket = zmq_socket (dI->context, ZMQ_PUSH)) == NULL )
//    {
//		perror("ERROR: Could not create 0MQ socket: ");
//		exit(9);
//	}

	dI->context = zmq_ctx_new ();
    dI->signalSocket = zmq_socket (dI->context, ZMQ_REQ);
    dI->eventSocket = zmq_socket (dI->context, ZMQ_PUSH);
    dI->dataSocket = zmq_socket (dI->context, ZMQ_PUSH);

    dI->filePart = 0;
    dI->responseTimeout    = 1000;

    char connectionStr[128];
    int rc;

    // Create sockets
    sprintf(connectionStr, "tcp://%s:%s", dI->signalHost, dI->signalPort);
    rc = zmq_connect(dI->signalSocket, connectionStr);
    assert (rc == 0);
//            self.log.error("Failed to start signalSocket (connect): '" + connectionStr + "'", exc_info=True)
    printf("signalSocket started (connect) for '%s'\n", connectionStr);

    sprintf(connectionStr, "tcp://localhost:%s", dI->eventPort);
    rc = zmq_connect(dI->eventSocket, connectionStr);
    assert (rc == 0);
//            self.log.error("Failed to start eventSocket (connect): '" + connectionStr + "'", exc_info=True)
    printf("eventSocket started (connect) for '%s'\n", connectionStr);

    sprintf(connectionStr, "tcp://localhost:%s", dI->dataPort);
    rc = zmq_connect(dI->dataSocket, connectionStr);
    assert (rc == 0);
//            self.log.error("Failed to start dataSocket (connect): '" + connectionStr + "'", exc_info=True)
    printf("dataSocket started (connect) for '%s'\n", connectionStr);

}


int dataIngest_createFile (dataIngest *dI, char *fileName)
{

//        if self.openFile and self.openFile != filename:
//            raise Exception("File " + str(filename) + " already opened.")

    char *message;
    int rc;

    // Send notification to receiver
    rc = s_send (dI->signalSocket, "OPEN_FILE");
    printf ("Sending signal to open a new file.\n");

    message = s_recv (dI->signalSocket);
    printf ("Received responce: %s\n", message);

    dI->filename = fileName;
    dI->filePart = 0;

    free (message);

}


int dataIngest_write (dataIngest *dI, char *data, int size)
//int dataIngest_write (dataIngest *dI, void *data, int &size)
{

    char message[128];
    int rc;

    sprintf(message, "{ \"filePart\": %d, \"filename\": \"%s\" }", dI->filePart, dI->filename);

    // Send event to eventDetector
    rc = s_send (dI->eventSocket, message);

    // Send data to ZMQ-Queue
    rc = s_send (dI->signalSocket, data);

    dI->filePart += 1;

};


int dataIngest_closeFile (dataIngest *dI)
{

    char *message = "CLOSE_FILE";
    char *answer;

    int rc;

    // Send close-signal to signal socket
    rc = s_send (dI->signalSocket, message);
    printf ("Sending signal to close the file to signalSocket.\n");
    //        self.log.error("Sending signal to close the file to signalSocket...failed.")


    // send close-signal to event Detector
    rc = s_send (dI->eventSocket, message);
    printf ("Sending signal to close the file to eventSocket.(sendMessage=%s)\n", message);
//        self.log.error("Sending signal to close the file to eventSocket...failed.)")


//    try:
//        socks = dict(self.poller.poll(10000)) # in ms
//    except:
//        socks = None
//        self.log.error("Could not poll for signal", exc_info=True)
//
//    if socks and self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:
    //  Get the reply.
    answer = s_recv (dI->signalSocket);
    printf ("Received answer to signal: %s\n", answer);
//    else:
//        recvMessage = None


    if ( message != answer )
    {
        printf ("recieved message: %s\n", answer);
        printf ("send message: %s\n", message);
//            raise Exception("Something went wrong while notifying to close the file")
    };

    dI->filename = "";
    dI->filePart = 0;

    free (message);
    free (answer);

};

int dataIngest_stop (dataIngest *dI)
{

    free (dI->signalHost);
    free (dI->extHost);
    free (dI->signalPort);
    free (dI->eventPort);
    free (dI->dataPort);

    free (dI->filename);

    printf ("closing signalSocket...");
    zmq_close(dI->signalSocket);
    printf ("closing eventSocket...");
    zmq_close(dI->eventSocket);
    printf ("closing dataSocket...");
    zmq_close(dI->dataSocket);
//            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

    printf ("Closing ZMQ context...");
    zmq_ctx_destroy(dI->context);
//                self.log.error("Closing ZMQ context...failed.", exc_info=True)

    //  Prepare our context and socket
    free (dI->context);
    free (dI->signalSocket);
    free (dI->eventSocket);
    free (dI->dataSocket);

};

//#endif


int main ()
{
    dataIngest *obj;
    char *data;
    int i;
    int size;

    dataIngest_init (obj);

    dataIngest_createFile (obj, "1.h5");

    for (i=0; i < 5; i++)
    {
        data = "asdfasdasdfasd";
        size = strlen(data);
        dataIngest_write (obj, data, size);
        printf ("write");
    };

    dataIngest_closeFile (obj);

    printf ("Stopping");
    dataIngest_stop(obj);

    return 0;
};


