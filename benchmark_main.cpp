#include "flexql.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

namespace {

constexpr int kInsertBatchSize = 1000000;
constexpr long long kTargetMicrosecondsPerRow = 2;
constexpr long long kTargetFixedOverheadMicroseconds = 100000;

int ignore_rows(void *, int, char **, char **) {
    return 0;
}

std::string scoped_name(const std::string &base) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return base + "_" + std::to_string(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

std::string build_insert_batch_sql(const std::string &table_name, int start_id, int row_count) {
    return "BENCHMARK INSERT INTO " + table_name +
           " START " + std::to_string(start_id) +
           " COUNT " + std::to_string(row_count) + ";";
}

double micros_to_seconds(long long elapsed_us) {
    return static_cast<double>(elapsed_us) / 1000000.0;
}

long long target_budget_microseconds(int rows) {
    return std::max(
        static_cast<long long>(rows) * kTargetMicrosecondsPerRow,
        kTargetFixedOverheadMicroseconds);
}

}  // namespace

int main(int argc, char **argv) {
    const char *host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? std::stoi(argv[2]) : 9000;
    int rows = (argc > 3) ? std::stoi(argv[3]) : 10000;
    const std::string table_name = scoped_name("bench");

    flexql_db *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "Unable to connect to server\n";
        return 1;
    }

    char *errmsg = nullptr;
    const std::string create_sql =
        "CREATE TABLE " + table_name +
        " (id INT PRIMARY KEY, name VARCHAR, email VARCHAR, balance DECIMAL, expires_at DECIMAL);";
    flexql_exec(db, create_sql.c_str(), nullptr, nullptr, &errmsg);
    flexql_free(errmsg);
    errmsg = nullptr;

    auto insert_start = std::chrono::steady_clock::now();
    for (int inserted = 0; inserted < rows; inserted += kInsertBatchSize) {
        const int batch_rows = std::min(kInsertBatchSize, rows - inserted);
        const std::string sql = build_insert_batch_sql(table_name, inserted + 1, batch_rows);
        if (flexql_exec(db, sql.c_str(), nullptr, nullptr, &errmsg) != FLEXQL_OK) {
            std::cerr << "Insert failed at row " << inserted << ": "
                      << (errmsg ? errmsg : "unknown") << "\n";
            flexql_free(errmsg);
            flexql_close(db);
            return 1;
        }
    }
    auto insert_end = std::chrono::steady_clock::now();

    auto select_start = std::chrono::steady_clock::now();
    const std::string select_sql = "SELECT * FROM " + table_name + " WHERE id = 42;";
    if (flexql_exec(db, select_sql.c_str(), ignore_rows, nullptr, &errmsg) != FLEXQL_OK) {
        std::cerr << "Select failed: " << (errmsg ? errmsg : "unknown") << "\n";
        flexql_free(errmsg);
        flexql_close(db);
        return 1;
    }
    auto select_end = std::chrono::steady_clock::now();

    const auto insert_us = std::chrono::duration_cast<std::chrono::microseconds>(insert_end - insert_start).count();
    const auto select_us = std::chrono::duration_cast<std::chrono::microseconds>(select_end - select_start).count();
    const auto budget_us = target_budget_microseconds(rows);

    std::cout << "Inserted " << rows << " rows in " << micros_to_seconds(insert_us) << " sec\n";
    std::cout << "Target budget: " << micros_to_seconds(budget_us) << " sec\n";
    std::cout << "Budget check: " << (insert_us <= budget_us ? "PASS" : "FAIL") << "\n";
    std::cout << "Point select completed in " << select_us << " us\n";

    flexql_close(db);
    return insert_us <= budget_us ? 0 : 1;
}
