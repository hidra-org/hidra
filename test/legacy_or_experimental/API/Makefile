test: test_hidraIngest.cpp
	g++ -o test_hidraIngest.o test_hidraIngest.cpp -lzmq -std=c++0x

example_hidraIngest_c: example_hidraIngest.c
	gcc -o example_hidraIngest.x example_hidraIngest.c ../../APIs/hidraIngest.c -lzmq -ljson-c -I/usr/include/json-c/ -I../../APIs

example_dataTransferAPI_c: example_dataTransferAPI_nexus.c
	gcc -o example_dataTransferAPI_nexus.x -ggdb example_dataTransferAPI_nexus.c ../../APIs/dataTransferAPI.c -lzmq -ljson-c -I/usr/include/json-c/ -I../../APIs
#	gcc -o example_dataTransferAPI_nexus.x `pkg-config --cflags glib-2.0` -ggdb example_dataTransferAPI_nexus.c ../../APIs/dataTransferAPI.c -lzmq -ljson-c -I/usr/include/json-c/ -I../../APIs `pkg-config --libs glib-2.0`
#
test_dataTransferAPI_c: test_dataTransferAPI.c
	gcc -o test_dataTransferAPI.x -ggdb test_dataTransferAPI.c -lzmq -ljson-c -I/usr/include/json-c/
