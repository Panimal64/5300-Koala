sql5300: sql5300.o 
	g++ -L/usr/local/berkeleydb/lib -L/usr/local/db6/lib -o $@ $< -ldb_cxx -lsqlparser

sql5300.o: sql5300.cpp   
	g++ -I/usr/local/db6/include -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -o $@ $<

clean:
	rm -f sql5300 *.o
