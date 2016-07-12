test: test_dataIngestAPI.cpp
	g++ -o test_dataIngestAPI.o test_dataIngestAPI.cpp -lzmq -std=c++0x

test_dataIngestAPI.x: test_dataIngestAPI.c
	gcc -o test_dataIngestAPI.x test_dataIngestAPI.c ../../APIs/dataIngestAPI.c -lzmq -I../../APIs

test_dataTransferAPI.x: test_dataTransferAPI_nexus.c
	gcc -o test_dataTransferAPI_nexus.x `pkg-config --cflags glib-2.0` -ggdb test_dataTransferAPI_nexus.c ../../APIs/dataTransferAPI.c -lzmq -ljson-c -I/usr/include/json-c/ -I../../APIs `pkg-config --libs glib-2.0`
