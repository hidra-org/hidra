#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

void cleanup(void* socket, void* context)
{
    zmq_close(socket);
    zmq_ctx_destroy(context);
}

int main () {
    char* signalHost = "*";
    char* signalPort = "6000";

    //  Prepare our context and socket
    void *context = zmq_ctx_new();
    void *socket = zmq_socket(context, ZMQ_REP);

//    char* connectionStr;
    char connectionStr[128];

    char* filename = "/space/projects/zeromq-data-transfer/data/source/local/5.cbf";
    int i = 0;

//    char* m;
    char m[128];
    char request[128];

    sprintf(connectionStr, "tcp://%s:%s", signalHost, signalPort);
    int rc = zmq_bind(socket, connectionStr);
    printf("Sender: Started\n");

    while (1)
    {

        printf("Receiving");
        //  Wait for next request from client
        int num = zmq_recv(socket, request, strlen(request), 0);

        if (num > 0)
        {
            request[num] = '\0';
            printf("Receiver: Received (%s)\n", request);
        }

        //  Do some 'work'
        sleep(1);

        //  Send reply back to client
        sprintf(m, "{ \"filePart\": %d, \"filename\": \"%s\" }", i, filename);
//        sprintf(m, "{ \"filePart\": %d, \"filename\": \"/space/projects/zeromq-data-transfer/data/source/local/5.cbf\" }", i);
        printf("Sender: Sending (%s)\n", m);

        int rc = zmq_send(socket, m, strlen(m), 0);

        printf("Sended\n");
        i++;
    }

    zmq_close(socket);
    zmq_ctx_destroy(context);

    return 0;
}
