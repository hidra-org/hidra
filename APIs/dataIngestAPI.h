// API to ingest data into a data transfer unit

#ifndef DATAINGEST_H
#define DATAINGEST_H

#define version "0.0.1"

typedef struct dataIngest dataIngest;

int dataIngest_init (dataIngest **dI);

int dataIngest_createFile (dataIngest *dI, char *fileName);

int dataIngest_write (dataIngest *dI, char *data, int size);

int dataIngest_closeFile (dataIngest *dI);

int dataIngest_stop (dataIngest *dI);

#endif
