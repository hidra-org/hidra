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

    rc = dataTransfer_init (&obj, "nexus");
    printf ("dataTransfer_init returned: %i\n", rc);

    rc = dataTransfer_start (obj);
    printf ("dataTransfer_start returned: %i\n", rc);

    rc = dataTransfer_read (obj, data, size);
    printf ("dataTransfer_read returned: %i\n", rc);
    printf("Read data: %s, size: %d\n", data, size);

    printf ("Stopping\n");
    rc = dataTransfer_stop(obj);
    printf ("dataTransfer_stop returned: %i\n", rc);

//    free(obj);

    return 0;
};


