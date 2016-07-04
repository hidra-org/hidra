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
#include <errno.h>


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
    printf("recieved string: %s\n", string);
    zmq_msg_close (&message);
    string [size] = 0;
    return string;
};


static int s_send (void * socket, char* string)
{
    printf("Sending: %s\n", string);
    int size = zmq_send (socket, string, strlen (string), 0);
    if (size == -1) {
        printf("Sending failed\n");
//        fprintf (stderr, "ERROR: Sending failed\n");
//        perror("");
    };
    return size;

};


typedef struct {
    char *extHost;
    char *localhost;

    char *signalPort;
    char *dataPort;

    //  Prepare our context and socket
    void *context;

    void *signalSocket;
    void *dataSocket;

    //  Initialize poll set
//    zmq::pollitem_t items [];


    int numberOfStreams;
//    self.recvdCloseFrom  = []
    char *replyToSignal;
    bool allCloseRecvd;

} nexusTransfer;


int nexusTransfer_init (nexusTransfer *nT)
{
    nT->extHost    = "0.0.0.0";
    nT->localhost  = "localhost";

    nT->signalPort = "50050";
    nT->dataPort   = "50100";

    if ( (nT->context = zmq_ctx_new ()) == NULL )
    {
		perror("ERROR: Cannot create 0MQ context: ");
		exit(9);
	}

	if ( (nT->signalSocket = zmq_socket (nT->context, ZMQ_REP)) == NULL )
    {
		perror("ERROR: Could not create 0MQ signalSocket: ");
		exit(9);
	}

    if ( (nT->dataSocket = zmq_socket (nT->context, ZMQ_PULL)) == NULL )
    {
		perror("ERROR: Could not create 0MQ dataSocket: ");
		exit(9);
	}

    nT->replyToSignal = ""
    nT->allCloseRecvd = false

    char connectionStr[128];
    int rc;

    // Create sockets
    snprintf(connectionStr, sizeof(connectionStr), "tcp://%s:%s", nT->extlHost, nT->signalPort);
//    rc = zmq_connect(nT->signalSocket, connectionStr);
//    assert (rc == 0);
    if ( zmq_bind(nT->signalSocket, connectionStr) )
    {
        fprintf (stderr, "ERROR: Could not start signalSocket (bind) for '%s'\n", connectionStr);
        perror("");
    } else {
        printf("signalSocket started (bind) for '%s'\n", connectionStr);
    }

    snprintf(connectionStr, sizeof(connectionStr), "tcp://%s:%s", nT->localhost, nT->dataPort);
//    rc = zmq_connect(nT->dataSocket, connectionStr);
//    assert (rc == 0);
    if ( zmq_bind(nT->dataSocket, connectionStr) )
    {
        fprintf (stderr, "ERROR: Could not start dataSocket (bind) for '%s'\n", connectionStr);
        perror("");
    } else {
        printf("dataSocket started (bind) for '%s'\n", connectionStr);
    }


    return 0;

}


int nexusTransfer_read (nexusTransfer *nT, char *data, int size)
{

    zmq_pollitem_t items [] = {
        { nT->signalSocket,   0, ZMQ_POLLIN, 0 },
        { nT->dataSocket, 0, ZMQ_POLLIN, 0 }
    };

    while (1)
    {
        printf ("polling");
        zmq_poll (items, 2, -1);

        if (items [0].revents & ZMQ_POLLIN)
        {
            printf ("signalSocket is polling\n");

            data = s_recv (dI->signalSocket);
            printf ("signalSocket recv: '%s'\n", message);

            if (data == b"CLOSE_FILE" && nT->allCloseRecvd == false):
                nT->replyToSignal = data;
            else:
                rc = zmq_send (nT-signalSocket, data, strlen(data), 0);
                printf ("signalSocket send: '%s'\n", data);

                return 0;
        }

        if (items [1].revents & ZMQ_POLLIN) {

            printf ("dataSocket is polling\n");
            char *multipartMessage[2];
            int j;

            rc = getMultipartMessage(dI->dataSocket, multipartMessage)

            for (j = 0; j < 2; j++)
            {
                free(multipartMessage[j]);
            };
        }

};


int recv_multipartMessage (void *socket, char **multipartMessage, int *len)
{
    int i = 0;
    int size;
    int more;
    size_t more_size = sizeof(more);
    while (1)
    {

        //  Wait for next request from client
        zmq_msg_t message;
        zmq_msg_init (&message);
        m_size = zmq_msg_recv (&message, socket, 0);

        //Process message
        multipartMessage[i] = malloc (m_size + 1);
        memcpy (multipartMessage[i], zmq_msg_data (&message), m_size);
        printf("recieved string: %s\n", multipartMessage[i]);
        multipartMessage[i] [m_size] = 0;

        i++;

        zmq_msg_close(&message);
        zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size);
        if (more == 0)
        {
            printf("last received\n");
            *len = i;
            break;
        };

    }

    return 0;


};



int getMultipartMessage (void *socket, char *data)
{
    char* mulitpartMessage[2];
    int len;
    int rc;

    //TODO multipart
    rc = recv_multipartMessage (dI-dataSocket, multipartMessage, len);
    printf ("multipartMessage[0]=%s\nmultipartMessage[1]=%s\n", multipartMessage[0], multipartMessage[1]);
    //self.log.error("Could not receive data due to unknown error.", exc_info=True)

    if (len < 2)
    {
        printf ("Received mutipart-message is too short. Either config or file content is missing.");
//            self.log.debug("multipartMessage=" + str(mutipartMessage))
        //TODO coorect errorcode
        return -1;


    }

    if (multipartMessage[0] == "CLOSE_FILE")
    {
/*
            id = multipartMessage[1]
            self.recvdCloseFrom.append(id)
            self.log.debug("Received close-file-signal from DataDispatcher-" + id)

            # get number of signals to wait for
            if not self.numberOfStreams:
                self.numberOfStreams = int(id.split("/")[1])

            # have all signals arrived?
            if len(self.recvdCloseFrom) == self.numberOfStreams:
                self.log.info("All close-file-signals arrived")
                self.allCloseRecvd = True
                if self.replyToSignal:
                    self.signalSocket.send(self.replyToSignal)
                    self.log.debug("signalSocket send: " + self.replyToSignal)
                    self.replyToSignal = False
                else:
                    pass

*/
        data = "CLOSE_FILE"
        return 0;

    } else {
        data = multipartMessage[1];
        return 0;
    }
};


int nexusTransfer_stop (dataIngest *nT)
{

    printf ("closing signalSocket...\n");
    zmq_close(nT->signalSocket);
    printf ("closing dataSocket...\n");
    zmq_close(nT->dataSocket);
//            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

    printf ("Closing ZMQ context...\n");
    zmq_ctx_destroy(nT->context);
//                self.log.error("Closing ZMQ context...failed.", exc_info=True)

//    free (nT);
    printf ("Cleanup finished.\n");

    return 0;
};

//#endif


int main ()
{
    nexusTransfer *obj;
    obj = malloc(sizeof(nexusTransfer));

    char *data;
    int i;
    int size;
    int rc;

    rc = nexusTransfer_init (obj);

    rc = nexusTransfer_read (obj, data, size);
    printf("Read data: %s, size: %d\n", data, size);

    for (i=0; i < 5; i++)
    {
        rc = nexusTransfer_read (obj, data, size);
        printf("Read data: %s, size: %d\n", data, size);
    };

    rc = nexusTransfer_read (obj, data, size);
    printf("Read data: %s, size: %d\n", data, size);

    printf ("Stopping\n");
    rc = nexusTransfer_stop(obj);

    free(obj);

    return 0;
};


