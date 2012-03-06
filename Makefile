CXX = g++
CPPFLAGS = -g -fpermissive -Wall -I. -I${HOME}/cs454/a2/include -I${HOME}/cs454/a2/include/thrift -Igen-cpp
LDFLAGS = -L${HOME}/cs454/a2/lib -lthrift -lpthread -lcrypto
LD = g++

PROGRAMS = server WatID_test

OBJECTS = WatDHTServer.o WatDHTHandler.o WatDHTState.o WatID.o\
	gen-cpp/WatDHT_constants.o gen-cpp/WatDHT.o gen-cpp/WatDHT_types.o

INCFILES = WatDHTHandler.h WatDHTServer.h WatDHTState.h WatID.h\
	gen-cpp/WatDHT_constants.h gen-cpp/WatDHT.h gen-cpp/WatDHT_types.h

all: $(PROGRAMS) $(OBJECTS) $(INCFILES)

server: $(OBJECTS)
	$(LD) $^ $(LDFLAGS) -o $@

WatID_test: WatID_test.o WatID.o
	$(LD) $^ $(LDFLAGS) -o $@

clean:
	rm -f *.o $(PROGRAMS) *~
