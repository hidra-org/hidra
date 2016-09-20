// API to ingest data into a HiDRA unit

#ifndef HIDRAINGEST_H
#define HIDRAINGEST_H

#define version "0.0.1"

typedef struct hidraIngest hidraIngest;

typedef enum { SUCCESS, NOTSUPPORTED, USAGEERROR, FORMATERROR, ZMQERROR, CONNECTIONFAILED, VERSIONERROR, AUTHENTICATIONFAILED, COMMUNICATIONFAILED, DATASAVINGERROR } HIDRA_ERROR;

HIDRA_ERROR hidraIngest_init (hidraIngest **dI);

HIDRA_ERROR hidraIngest_createFile (hidraIngest *dI, char *fileName);

HIDRA_ERROR hidraIngest_write (hidraIngest *dI, char *data, int size);

HIDRA_ERROR hidraIngest_closeFile (hidraIngest *dI);

HIDRA_ERROR hidraIngest_stop (hidraIngest *dI);

#endif
