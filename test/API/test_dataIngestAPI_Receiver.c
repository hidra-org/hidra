//
#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

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
}

static int s_send (void * socket, char* string)
{
    int size = zmq_send (socket, string, strlen (string), 0);
    return size;

}


int main() {

    char *extHost    = "0.0.0.0";
    char *signalPort = "50050";
    char *eventPort  = "50003";
    char *dataPort   = "50010";


    //  Prepare the context and socket
    void *context = zmq_ctx_new();
    void *signalSocket = zmq_socket(context, ZMQ_REP);
    void *eventSocket = zmq_socket(context, ZMQ_PULL);
    void *dataSocket = zmq_socket(context, ZMQ_PULL);

    char connectionStr[128];

    int i;
    int rc;
    int size;

    char *message;
    char *event;
    char *data;

    // Create sockets
    sprintf(connectionStr, "tcp://%s:%s", extHost, signalPort);
    rc = zmq_bind(signalSocket, connectionStr);
    assert (rc == 0);
    printf("signalSocket started (bind) for '%s'\n", connectionStr);

    sprintf(connectionStr, "tcp://%s:%s", extHost, eventPort);
    rc = zmq_bind(eventSocket, connectionStr);
    assert (rc == 0);
    printf("eventSocket started (bind) for '%s'\n", connectionStr);

    sprintf(connectionStr, "tcp://%s:%s", extHost, dataPort);
    rc = zmq_bind(dataSocket, connectionStr);
    assert (rc == 0);
    printf("dataSocket started (bind) for '%s'\n", connectionStr);

    // Receive signal
    message = s_recv (signalSocket);
    printf("signalSocket recv: '%s'\n", message);

    // Send confirmation
    rc = s_send (signalSocket, message);
    printf ("signalSocket send: %s\n", message);

    // Receive data
    for (i = 0; i < 5; i++)
    {
        event = s_recv (eventSocket);
        printf ("eventSocket recv: '%s'\n", event);

        data = s_recv (dataSocket);
        printf ("dataSocket recv: '%s'\n", data);
    }

    // Receive signal
    message = s_recv (signalSocket);
    printf ("signalSocket recv: '%s'\n", message);

    // Send confirmation
    rc = s_send (signalSocket, message);
    printf("signalSocket send: %s\n", message);

    data = s_recv (eventSocket);
    printf("eventSocket recv: '%s'\n", data);

    free(message);
    free(event);
    free(data);

    zmq_close(signalSocket);
    zmq_close(eventSocket);
    zmq_close(dataSocket);
    zmq_ctx_destroy(context);

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
