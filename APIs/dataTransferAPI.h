// API to ingest data into a data transfer unit

#ifndef DATATRANSFER_H
#define DATATRANSFER_H

#define version "0.0.1"

typedef struct dataTransfer dataTransfer;

int dataTransfer_init (dataTransfer **nT);

int dataTransfer_initiate (dataTransfer *nT, char **targets);

int dataTransfer_read (dataTransfer *nT, char *data, int size);

int dataTransfer_stop (dataTransfer *nT);

#endif
