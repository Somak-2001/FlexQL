CXX := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pedantic -pthread

COMMON := engine.cpp protocol.cpp

all: flexql_server flexql_repl flexql_benchmark smoke_test

flexql_server: server_main.cpp $(COMMON) engine.h protocol.h
	$(CXX) $(CXXFLAGS) -o $@ server_main.cpp $(COMMON)

flexql_repl: repl_main.cpp flexql.cpp $(COMMON) flexql.h engine.h protocol.h
	$(CXX) $(CXXFLAGS) -o $@ repl_main.cpp flexql.cpp $(COMMON)

flexql_benchmark: benchmark_main.cpp flexql.cpp $(COMMON) flexql.h engine.h protocol.h
	$(CXX) $(CXXFLAGS) -o $@ benchmark_main.cpp flexql.cpp $(COMMON)

smoke_test: smoke_test.cpp $(COMMON) engine.h protocol.h
	$(CXX) $(CXXFLAGS) -o $@ smoke_test.cpp $(COMMON)

clean:
	rm -f flexql_server flexql_repl flexql_benchmark smoke_test
