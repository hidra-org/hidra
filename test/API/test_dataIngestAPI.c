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

    dataIngest_init (obj);

    dataIngest_createFile (obj, "1.h5");

    for (i=0; i < 5; i++)
    {
        data = "asdfasdasdfasd";
        size = strlen(data);
        dataIngest_write (obj, data, size);
        printf ("write\n");
    };

    dataIngest_closeFile (obj);

    printf ("Stopping\n");
    dataIngest_stop(obj);

    return 0;

};
