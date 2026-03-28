#include "flexql.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

namespace {

int ignore_rows(void *, int, char **, char **) {
    return 0;
}

}  // namespace

int main(int argc, char **argv) {
    const char *host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? std::stoi(argv[2]) : 9000;
    int rows = (argc > 3) ? std::stoi(argv[3]) : 10000;

    flexql_db *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "Unable to connect to server\n";
        return 1;
    }

    char *errmsg = nullptr;
    flexql_exec(db, "CREATE TABLE bench (id INT PRIMARY KEY, score DECIMAL, name VARCHAR);", nullptr, nullptr, &errmsg);
    flexql_free(errmsg);
    errmsg = nullptr;

    auto insert_start = std::chrono::steady_clock::now();
    for (int i = 0; i < rows; ++i) {
        std::string sql = "INSERT INTO bench VALUES (" + std::to_string(i) + ", " +
                          std::to_string(i * 0.5) + ", 'user_" + std::to_string(i) + "');";
        if (flexql_exec(db, sql.c_str(), nullptr, nullptr, &errmsg) != FLEXQL_OK) {
            std::cerr << "Insert failed at row " << i << ": " << (errmsg ? errmsg : "unknown") << "\n";
            flexql_free(errmsg);
            flexql_close(db);
            return 1;
        }
    }
    auto insert_end = std::chrono::steady_clock::now();

    auto select_start = std::chrono::steady_clock::now();
    if (flexql_exec(db, "SELECT * FROM bench WHERE id = 42;", ignore_rows, nullptr, &errmsg) != FLEXQL_OK) {
        std::cerr << "Select failed: " << (errmsg ? errmsg : "unknown") << "\n";
        flexql_free(errmsg);
        flexql_close(db);
        return 1;
    }
    auto select_end = std::chrono::steady_clock::now();

    const auto insert_ms = std::chrono::duration_cast<std::chrono::milliseconds>(insert_end - insert_start).count();
    const auto select_us = std::chrono::duration_cast<std::chrono::microseconds>(select_end - select_start).count();

    std::cout << "Inserted " << rows << " rows in " << insert_ms << " ms\n";
    std::cout << "Point select completed in " << select_us << " us\n";

    flexql_close(db);
    return 0;
}
