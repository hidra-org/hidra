// API to ingest data into a data transfer unit

#include "dataTransferAPI.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int open_cb (params_cb_t *cbp, char *message)
{
    printf("execute openCall_cb\n");
}

int read_cb (params_cb_t *cbp, struct json_object *metadata, char *payload)
{
    printf("execute readCall_cb\n");
}

int close_cb (params_cb_t *cbp, char **multipartMessage)
{
    printf("execute closeCall_cb\n");
}

struct params_cb
{
    int num;
};


int main ()
{
    dataTransfer_t *obj;

    char *data;
    int i;
    int size;
    int rc;
    params_cb_t *cbp;

    rc = dataTransfer_init (&obj, "nexus");
    if (rc) exit(-9);
    printf ("dataTransfer_init returned: %i\n", rc);

    rc = dataTransfer_start (obj);
    printf ("dataTransfer_start returned: %i\n", rc);

//    rc = dataTransfer_read (obj, data, size);
    rc = dataTransfer_read (obj, cbp, open_cb, read_cb, close_cb);
    printf ("dataTransfer_read returned: %i\n", rc);
    printf("Read data: %s, size: %d\n", data, size);

    printf ("Stopping\n");
    rc = dataTransfer_stop(obj);
    printf ("dataTransfer_stop returned: %i\n", rc);

    return 0;
};


