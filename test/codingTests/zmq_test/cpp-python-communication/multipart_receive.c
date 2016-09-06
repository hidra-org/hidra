#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>


int getMultipartMessage (void *socket, char **multipartMessage, int *len)
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
        size = zmq_msg_recv (&message, socket, 0);

        //Process message
        multipartMessage[i] = malloc (size + 1);
        memcpy (multipartMessage[i], zmq_msg_data (&message), size);
        printf("recieved string: %s\n", multipartMessage[i]);
        multipartMessage[i][size] = 0;

        i++;

        zmq_msg_close(&message);
        zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size);
        if(more == 0)
        {
            printf("last received\n");
            *len = i;
            break;
        };

    }

    return 0;


};

int main () {
    char* signalHost = "*";
    char* signalPort = "6000";

    //  Prepare our context and socket
    void *context = zmq_ctx_new();
    void *socket = zmq_socket(context, ZMQ_PULL);

//    char* connectionStr;
    char connectionStr[128];

    char m[128];
    char request[128];
    int rc;

    sprintf(connectionStr, "tcp://%s:%s", signalHost, signalPort);
    rc = zmq_bind(socket, connectionStr);
    printf("Receiver: Started\n");

    char *multipartMessage[2];
    int i = 0;
    int j;
    int len;
    while (1)
    {

        getMultipartMessage(socket, multipartMessage, &len);
        printf("len=%d\n", len);

        for (j = 0; j < 2; j++)
        {
            free(multipartMessage[j]);
        };
        i++;

    }

    zmq_close(socket);
    zmq_ctx_destroy(context);

    return 0;
}
