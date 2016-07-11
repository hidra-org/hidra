// API to ingest data into a data transfer unit

#include "dataTransferAPI.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>


int main ()
{
    dataTransfer *obj;

    char *data;
    int i;
    int size;
    int rc;

    rc = dataTransfer_init (&obj);

    rc = dataTransfer_start (obj);

    rc = dataTransfer_read (obj, data, size);
    printf("Read data: %s, size: %d\n", data, size);

    printf ("Stopping\n");
    rc = dataTransfer_stop(obj);

    free(obj);

    return 0;
};


