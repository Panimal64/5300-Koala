#paths
CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib

# following is a list of all the compiled object files needed to build the sql5300 executable
OBJS       = sql5300.o heap_storage.o test_heap_storage.o

# Rule for linking
sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $(OBJS) -ldb_cxx -lsqlparser

sql5300.o : heap_storage.h storage_engine.h
heap_storage.o : heap_storage.h storage_engine.h
test_heap_storage.o : heap_storage.h storage_engine.h

# General rule 
%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"


#clean file
clean:
	rm -f sql5300 *.o heap_storage *.o test_heap_storage *.o

