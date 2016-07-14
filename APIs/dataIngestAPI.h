// API to ingest data into a data transfer unit

#ifndef DATAINGEST_H
#define DATAINGEST_H

#define version "0.0.1"

typedef struct dataIngest dataIngest;

typedef enum { SUCCESS, NOTSUPPORTED, USAGEERROR, FORMATERROR, ZMQERROR, CONNECTIONFAILED, VERSIONERROR, AUTHENTICATIONFAILED, COMMUNICATIONFAILED, DATASAVINGERROR } HIDRA_ERROR;

HIDRA_ERROR dataIngest_init (dataIngest **dI);

HIDRA_ERROR dataIngest_createFile (dataIngest *dI, char *fileName);

HIDRA_ERROR dataIngest_write (dataIngest *dI, char *data, int size);

HIDRA_ERROR dataIngest_closeFile (dataIngest *dI);

HIDRA_ERROR dataIngest_stop (dataIngest *dI);

#endif
