//
#include "dataIngestAPI.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main()
{
    dataIngest *obj;

    char *data;
    int i;
    int size;
    int rc;

    rc = dataIngest_init (&obj);

    rc = dataIngest_createFile (obj, "1.h5");

    for (i=0; i < 5; i++)
    {
        data = "asdfasdasdfasd";
        size = strlen(data);
        rc = dataIngest_write (obj, data, size);
        printf ("write\n");
    };

    rc = dataIngest_closeFile (obj);

    printf ("Stopping\n");
    rc = dataIngest_stop(obj);

    free (obj);

    return 0;


};
