test: test_dataIngestAPI.cpp
	g++ -o test_dataIngestAPI.o test_dataIngestAPI.cpp -lzmq -std=c++0x

example_dataIngestAPI_c: example_dataIngestAPI.c
	gcc -o example_dataIngestAPI.x example_dataIngestAPI.c ../../APIs/dataIngestAPI.c -lzmq -I../../APIs

example_dataTransferAPI_c: example_dataTransferAPI_nexus.c
	gcc -o example_dataTransferAPI_nexus.x -ggdb example_dataTransferAPI_nexus.c ../../APIs/dataTransferAPI.c -lzmq -ljson-c -I/usr/include/json-c/ -I../../APIs
#	gcc -o example_dataTransferAPI_nexus.x `pkg-config --cflags glib-2.0` -ggdb example_dataTransferAPI_nexus.c ../../APIs/dataTransferAPI.c -lzmq -ljson-c -I/usr/include/json-c/ -I../../APIs `pkg-config --libs glib-2.0`
