test: test_dataIngestAPI.cpp
	g++ -o test_dataIngestAPI.o test_dataIngestAPI.cpp -lzmq -std=c++0x

test_dataIngestAPI.x: test_dataIngestAPI.c
	gcc -o test_dataIngestAPI.x test_dataIngestAPI.c ../../APIs/dataIngestAPI.c -lzmq -I../../APIs
