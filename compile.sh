g++ -std=c++17 -O3 -Wall -Wextra -pedantic -pthread \
  flexql_server.cpp engine.cpp protocol.cpp -o server_bin && \
g++ -std=c++17 -O3 -Wall -Wextra -pedantic -pthread \
  flexql.cpp protocol.cpp benchmark_flexql.cpp -o benchmark_bin && \
g++ -std=c++17 -O3 -Wall -Wextra -pedantic -pthread \
  benchmark_embedded.cpp engine.cpp -o benchmark_embedded_bin && \
cp server_wrapper.sh server.tmp && \
cp benchmark_wrapper.sh benchmark.tmp && \
chmod +x server.tmp benchmark.tmp && \
mv -f server.tmp server && \
mv -f benchmark.tmp benchmark
